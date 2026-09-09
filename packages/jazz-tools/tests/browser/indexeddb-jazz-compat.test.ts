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
import historicalCorpus from "../../fixtures/epoch-1-browser-jazz-corpus.json?raw";
import currentCorpus from "../../fixtures/current-browser-jazz-corpus.json?raw";
import { jazzStorageCorpusBrowserCommands } from "./browser-commands.js";
import { createAccountManager, ReadTier, schema as s, type DbConfig } from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import { accountRegistryUrl } from "../../src/accounts/context.js";
import { type Db } from "../../src/runtime/db.js";
import {
  INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY,
  INDEXEDDB_BTREE_DATABASE_VERSION,
  INDEXEDDB_BTREE_METADATA_STORE,
  INDEXEDDB_BTREE_PAGES_STORE,
  INDEXEDDB_STORAGE_MANIFEST,
  INDEXEDDB_STORAGE_MANIFEST_KEY,
  INDEXEDDB_STORAGE_MANIFEST_STORE,
  IndexedDbPageStore,
} from "../../src/runtime/indexeddb-page-store.js";
import {
  blockJazzServerNetwork,
  getJazzServerInfo,
  unblockJazzServerNetwork,
} from "./testing-server.js";
import {
  createBrowserTestDb as createDb,
  sleep,
  TestCleanup,
  uniqueDbName,
  withTimeout,
} from "./support.js";

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
  const openDbLabels = new Map<Db, string>();
  let pinnedCorpusPhase = "not started";

  function receipt(stage: string): void {
    console.info(`[jazz-browser-corpus-phase] ${stage}`);
  }

  async function pinnedPhase<T>(stage: string, operation: () => Promise<T>): Promise<T> {
    pinnedCorpusPhase = `${stage}:start`;
    receipt(pinnedCorpusPhase);
    try {
      const result = await withTimeout(
        operation(),
        15_000,
        `browser corpus phase ${stage} stalled`,
      );
      pinnedCorpusPhase = `${stage}:done`;
      receipt(pinnedCorpusPhase);
      return result;
    } catch (error) {
      pinnedCorpusPhase = `${stage}:failed`;
      receipt(pinnedCorpusPhase);
      throw error;
    }
  }

  async function shutdownTrackedDb(db: Db, label: string): Promise<void> {
    receipt(`shutdown:${label}:start; pinned-phase=${pinnedCorpusPhase}`);
    await withTimeout(db.shutdown(), 5_000, `browser corpus shutdown ${label} stalled`);
    receipt(`shutdown:${label}:done; pinned-phase=${pinnedCorpusPhase}`);
    openDbLabels.delete(db);
  }

  afterEach(async () => {
    receipt(
      `cleanup:start; pinned-phase=${pinnedCorpusPhase}; open=${openDbs
        .map((db) => openDbLabels.get(db) ?? "unlabeled")
        .join(",")}`,
    );
    await Promise.all(
      openDbs
        .splice(0)
        .reverse()
        .map((db) => shutdownTrackedDb(db, openDbLabels.get(db) ?? "unlabeled")),
    );
    receipt(`cleanup:dbs-done; pinned-phase=${pinnedCorpusPhase}`);
    await Promise.all([...databaseNames].map((name) => IndexedDbPageStore.destroy(name)));
    databaseNames.clear();
    receipt(`cleanup:done; pinned-phase=${pinnedCorpusPhase}`);
  });

  it("produces the current catalogue/history/branch/large-value corpus through public WasmDb", async () => {
    const server = await getJazzServerInfo("ba96582c-7167-5f52-ba63-3ebefe1c2b96");
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: app.wasmSchema,
      permissions,
    });
    const dbName = uniqueDbName("browser-storage-current-producer");
    const config = await persistentConfig(
      dbName,
      "jazz-auth-v1:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
      server,
    );
    let db = await openPersistentDb(config);
    const physicalDbName = await trackPhysicalDatabase(dbName);
    const body = "large value ".repeat(20_000);
    const initial = await db.transaction((tx) => {
      const project = tx.insert(app.projects, { name: "corpus project" });
      const main = tx.insert(
        app.documents,
        {
          branch: "main",
          title: "original title",
          projectId: project.id,
          body,
        },
        { branch: "main" },
      );
      tx.insert(
        app.documents,
        {
          branch: "draft",
          title: "draft override",
          projectId: project.id,
          body,
        },
        { branch: "draft" },
      );
      return main.id;
    });
    await withTimeout(
      initial.wait({ tier: "edge" }),
      20_000,
      "corpus initial write did not settle",
    );
    await withTimeout(
      db
        .update(app.documents, initial.value, { title: "current title" }, { branch: "main" })
        .wait({ tier: "edge" }),
      20_000,
      "corpus history update did not settle",
    );
    expect(await db.all(app.documents, { tier: "edge", branch: "main" })).toHaveLength(1);
    expect(await db.all(app.documents, { tier: "edge", branch: "draft" })).toHaveLength(1);
    await db.shutdown();
    openDbs.splice(openDbs.indexOf(db), 1);
    await sleep(100);
    const candidate = await rawRecords(physicalDbName);
    expect(
      rawManifest(candidate).find(([key]) => key === INDEXEDDB_STORAGE_MANIFEST_KEY)?.[1],
    ).toEqual(INDEXEDDB_STORAGE_MANIFEST);
    await blockJazzServerNetwork(server.serverUrl);
    try {
      db = await openPersistentDb(config);
      await db.disconnect();
      expect(
        await db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "main" }),
      ).toMatchObject([{ branch: "main", title: "current title", body }]);
      expect(
        await db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "draft" }),
      ).toMatchObject([{ branch: "draft", title: "draft override", body }]);
      await db.shutdown();
      openDbs.splice(openDbs.indexOf(db), 1);
    } finally {
      await unblockJazzServerNetwork(server.serverUrl);
    }
    // Export only after actual public reopen has validated the raw candidate.
    await jazzStorageCorpusBrowserCommands().writeBrowserStorageCorpus(candidate);
  }, 90_000);

  it("opens the pinned catalogue/history/branch/large-value corpus through public WasmDb", async () => {
    pinnedCorpusPhase = "pinned-test:start";
    receipt(pinnedCorpusPhase);
    const rawBeforeReadOnlyInspection = JSON.parse(currentCorpus) as Record<string, string>;
    const owner = JSON.parse(
      rawManifest(rawBeforeReadOnlyInspection).find(
        ([key]) => key === INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY,
      )![1] as string,
    );
    expect(owner).toMatchObject({
      version: 1,
      appId: "ba96582c-7167-5f52-ba63-3ebefe1c2b96",
      env: "dev",
      auth: { kind: "account" },
    });
    const registry = new URL(owner.auth.registry);
    const registrySuffix = `/apps/${owner.appId}/accounts`;
    expect(registry.pathname.endsWith(registrySuffix)).toBe(true);
    const serverBase = new URL(registry);
    serverBase.pathname = registry.pathname.slice(0, -registrySuffix.length);
    expect(accountRegistryUrl(serverBase.href, owner.appId)).toBe(owner.auth.registry);
    // Registry authority is part of the stored owner. Reopen under the exact
    // producer authority, even though that test server no longer exists. A
    // newly allocated test-server port is a different account namespace.
    const accounts = await createAccountManager({
      appId: owner.appId,
      serverUrl: serverBase.href,
    });
    const dbName = "browser-storage-compat-historical-root-v1";
    const secret = "jazz-auth-v1:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
    const account = accounts.restoreLocalFirst(secret);
    expect(account.id).toBe(owner.auth.account);
    // Omit serverUrl on the context: every pinned read and append is local,
    // including the first open, so upstream cannot repair a missing page.
    const config: DbConfig = {
      appId: owner.appId,
      account,
      driver: { type: "persistent", dbName },
    };
    // Prepare the real key-bound account before creating a context. Provision
    // once through the public path to identify its account-scoped physical root
    // before installing the pinned receipt.
    const bootstrap = await pinnedPhase("bootstrap-open", () =>
      openPersistentDb(config, "pinned-bootstrap"),
    );
    const physicalDbName = await pinnedPhase("physical-root", () => trackPhysicalDatabase(dbName));
    await pinnedPhase("bootstrap-shutdown", () => shutdownTrackedDb(bootstrap, "pinned-bootstrap"));
    openDbs.splice(openDbs.indexOf(bootstrap), 1);
    await pinnedPhase("bootstrap-settle", () => sleep(100));
    await pinnedPhase("install-pinned-records", () =>
      installRawRecords(physicalDbName, rawBeforeReadOnlyInspection),
    );
    expect(await pinnedPhase("read-installed-records", () => rawRecords(physicalDbName))).toEqual(
      rawBeforeReadOnlyInspection,
    );

    let db = await pinnedPhase("readonly-open", () => openPersistentDb(config, "pinned-readonly"));
    // The persistent replica lives in the worker. Use the public local-first
    // read across that hop. With no serverUrl, the worker cannot contact Core.
    // Inspector LocalOnly reads only the foreground's in-memory rows.
    const rawWhileReopened = await pinnedPhase("read-open-records", () =>
      rawRecords(physicalDbName),
    );
    // Reopen must materialize the durable local replica without depending on
    // a fresh remote-coverage round trip. The earlier edge read proves the
    // synced fixture; this is specifically the offline persistence boundary.
    const reopenedMain = await pinnedPhase("readonly-main-query", () =>
      db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "main" }),
    );
    const reopenedDraft = await pinnedPhase("readonly-draft-query", () =>
      db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "draft" }),
    );
    expect(reopenedMain).toHaveLength(1);
    expect(reopenedDraft).toHaveLength(1);
    expect(reopenedMain).toMatchObject({
      0: { branch: "main", title: "current title", body: "large value ".repeat(20_000) },
    });
    expect(reopenedDraft).toMatchObject({
      0: { branch: "draft", title: "draft override", body: "large value ".repeat(20_000) },
    });
    await pinnedPhase("readonly-shutdown", () => shutdownTrackedDb(db, "pinned-readonly"));
    openDbs.splice(openDbs.indexOf(db), 1);
    await pinnedPhase("readonly-settle", () => sleep(100));
    const rawAfterReadOnlyInspection = await pinnedPhase("read-post-readonly-records", () =>
      rawRecords(physicalDbName),
    );
    expectForegroundLeaseLifecycle(
      rawBeforeReadOnlyInspection,
      rawWhileReopened,
      rawAfterReadOnlyInspection,
    );
    expect(normalizeRuntimeLeaseRecords(rawAfterReadOnlyInspection)).toEqual(
      normalizeRuntimeLeaseRecords(rawBeforeReadOnlyInspection),
    );

    // This planted high-water regression must remain visible through the raw
    // receipt. In particular, normalization may hide a fresh opaque lease
    // token, but never a node identity, retired set, or HLC floor.
    const plantedLeaseRegression = corruptReusableLeaseHighWater(rawAfterReadOnlyInspection);
    expect(normalizeRuntimeLeaseRecords(plantedLeaseRegression)).not.toEqual(
      normalizeRuntimeLeaseRecords(rawAfterReadOnlyInspection),
    );

    // Append with today's writer only after the historical read-only/raw-byte
    // receipt above. Both generations must survive a fresh public open of
    // this same authenticated principal root.
    db = await pinnedPhase("writer-open", () => openPersistentDb(config, "pinned-writer"));
    const historicalProjects = await pinnedPhase("writer-projects-query", () =>
      db.all(app.projects, { tier: ReadTier.LocalFirst }),
    );
    const currentBody = "current writer large value ".repeat(12_000);
    const currentWrite = await pinnedPhase("writer-transaction", () =>
      db.transaction((tx) => {
        const project = tx.insert(app.projects, { name: "current writer project" });
        const document = tx.insert(
          app.documents,
          {
            branch: "main",
            title: "current writer document",
            projectId: project.id,
            body: currentBody,
          },
          { branch: "main" },
        );
        return { project, document };
      }),
    );
    await pinnedPhase("writer-local-settlement", () =>
      withTimeout(
        currentWrite.wait({ tier: "local" }),
        10_000,
        "current corpus write did not settle locally",
      ),
    );
    await pinnedPhase("writer-shutdown", () => shutdownTrackedDb(db, "pinned-writer"));
    openDbs.splice(openDbs.indexOf(db), 1);
    await pinnedPhase("writer-settle", () => sleep(100));
    const rawAfterCurrentWrite = await pinnedPhase("read-post-writer-records", () =>
      rawRecords(physicalDbName),
    );
    expect(rawAfterCurrentWrite[INDEXEDDB_BTREE_PAGES_STORE]).not.toEqual(
      rawAfterReadOnlyInspection[INDEXEDDB_BTREE_PAGES_STORE],
    );

    // Network isolation makes this a persistence receipt: the server cannot
    // repair lost current pages before the post-reopen assertions.
    await pinnedPhase("block-network", () => blockJazzServerNetwork(registry.origin));
    try {
      db = await pinnedPhase("offline-open", () => openPersistentDb(config, "pinned-offline"));
      expect(await pinnedPhase("offline-physical-root", () => trackPhysicalDatabase(dbName))).toBe(
        physicalDbName,
      );
      const mixedMain = await pinnedPhase("offline-main-query", () =>
        db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "main" }),
      );
      const mixedDraft = await pinnedPhase("offline-draft-query", () =>
        db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "draft" }),
      );
      const mixedProjects = await pinnedPhase("offline-projects-query", () =>
        db.all(app.projects, { tier: ReadTier.LocalFirst }),
      );
      expect(mixedMain).toHaveLength(2);
      expect(mixedMain).toEqual(expect.arrayContaining(reopenedMain));
      expect(mixedMain).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            title: "current writer document",
            branch: "main",
            body: currentBody,
          }),
        ]),
      );
      expect(mixedDraft).toEqual(reopenedDraft);
      expect(mixedProjects).toHaveLength(historicalProjects.length + 1);
      expect(mixedProjects).toEqual(expect.arrayContaining(historicalProjects));
      const currentProject = mixedProjects.find(
        (project) => project.name === "current writer project",
      );
      expect(currentProject).toBeDefined();
      expect(
        mixedMain.find((document) => document.title === "current writer document")?.projectId,
      ).toBe(currentProject!.id);
    } finally {
      await pinnedPhase("unblock-network", () => unblockJazzServerNetwork(registry.origin));
    }
    pinnedCorpusPhase = "pinned-test:complete";
    receipt(pinnedCorpusPhase);
  }, 90_000);

  it("rejects the historical retired-result codec profile without rewriting its pages", async () => {
    const server = await getJazzServerInfo("ba96582c-7167-5f52-ba63-3ebefe1c2b96");
    const dbName = uniqueDbName("browser-storage-retired-profile");
    const config = await persistentConfig(
      dbName,
      "jazz-auth-v1:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
      server,
    );
    const bootstrap = await openPersistentDb(config);
    const physicalDbName = await trackPhysicalDatabase(dbName);
    await bootstrap.shutdown();
    openDbs.splice(openDbs.indexOf(bootstrap), 1);
    await sleep(100);
    const historical = JSON.parse(historicalCorpus) as Record<string, string>;
    await installRawRecords(physicalDbName, historical);
    await expect(
      withTimeout(createDb(config), 5_000, "retired profile open did not reject"),
    ).rejects.toThrow("Missing or invalid IndexedDB storage epoch manifest");
    expect(await rawRecords(physicalDbName)).toEqual(historical);
  }, 30_000);

  it("rejects a corrupt durable epoch before handing a public Jazz handle to the app", async () => {
    const cleanup = new TestCleanup();
    const dbName = databaseName();
    const appId = "browser-storage-compat-corruption";
    const accounts = await createAccountManager({
      appId,
      serverUrl: "https://storage-fixture.example",
    });
    const config = {
      appId,
      account: accounts.createLocalFirst(),
      driver: { type: "persistent" as const, dbName },
    } satisfies DbConfig;
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

  async function persistentConfig(
    dbName: string,
    secret: string,
    server: Awaited<ReturnType<typeof getJazzServerInfo>>,
  ): Promise<DbConfig> {
    const accounts = await createAccountManager({
      appId: server.appId,
      serverUrl: server.serverUrl,
    });
    return {
      appId: server.appId,
      serverUrl: server.serverUrl,
      account: accounts.restoreLocalFirst(secret),
      driver: { type: "persistent", dbName },
    };
  }

  async function openPersistentDb(config: DbConfig, label = "unlabeled"): Promise<Db> {
    const db = await createDb(config);
    openDbs.push(db);
    openDbLabels.set(db, label);
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

/**
 * A clean foreground shutdown returns its node lease to the durable owner;
 * reopening claims it again. Only the opaque random lease token is expected
 * to change. Node identity, HLC high-water, and retired-node history remain
 * part of the durable compatibility surface.
 */
function normalizeRuntimeLeaseRecords(records: Record<string, string>): Record<string, string> {
  const manifest = rawManifest(records);
  return {
    ...records,
    [INDEXEDDB_STORAGE_MANIFEST_STORE]: JSON.stringify(
      manifest.map(([key, value]) =>
        key === "foreground-node-leases-v1"
          ? [key, normalizeForegroundNodeLeasePool(value)]
          : [key, value],
      ),
    ),
  };
}

type RawForegroundNodeLease = {
  leaseId: string;
  node: unknown;
  confirmedTxTime: string;
};

type RawForegroundNodeLeasePool = {
  format: string;
  active: RawForegroundNodeLease[];
  reusable: RawForegroundNodeLease[];
  retired: unknown[];
};

function rawManifest(records: Record<string, string>): [string, unknown][] {
  const manifest = JSON.parse(records[INDEXEDDB_STORAGE_MANIFEST_STORE] ?? "[]") as unknown;
  if (!Array.isArray(manifest) || !manifest.every(isRawManifestEntry)) {
    throw new Error("expected raw IndexedDB manifest entries");
  }
  return manifest;
}

function isRawManifestEntry(value: unknown): value is [string, unknown] {
  return Array.isArray(value) && value.length === 2 && typeof value[0] === "string";
}

function foregroundNodeLeasePool(records: Record<string, string>): RawForegroundNodeLeasePool {
  const entry = rawManifest(records).find(([key]) => key === "foreground-node-leases-v1");
  if (!entry || !isRawForegroundNodeLeasePool(entry[1])) {
    throw new Error("expected a valid raw foreground-node lease pool");
  }
  return entry[1];
}

function isRawForegroundNodeLeasePool(value: unknown): value is RawForegroundNodeLeasePool {
  if (!value || typeof value !== "object") return false;
  const pool = value as Partial<RawForegroundNodeLeasePool>;
  return (
    typeof pool.format === "string" &&
    Array.isArray(pool.active) &&
    Array.isArray(pool.reusable) &&
    Array.isArray(pool.retired) &&
    [...pool.active, ...pool.reusable].every(isRawForegroundNodeLease)
  );
}

function isRawForegroundNodeLease(value: unknown): value is RawForegroundNodeLease {
  if (!value || typeof value !== "object") return false;
  const lease = value as Partial<RawForegroundNodeLease>;
  return (
    typeof lease.leaseId === "string" &&
    typeof lease.confirmedTxTime === "string" &&
    lease.node !== undefined
  );
}

function normalizeForegroundNodeLeasePool(value: unknown): RawForegroundNodeLeasePool {
  if (!isRawForegroundNodeLeasePool(value)) {
    throw new Error("expected a valid raw foreground-node lease pool");
  }
  const normalizeLease = ({ leaseId: _leaseId, ...lease }: RawForegroundNodeLease) => ({
    ...lease,
    leaseId: "<opaque-random-lease-id>",
  });
  return {
    ...value,
    active: value.active.map(normalizeLease),
    reusable: value.reusable.map(normalizeLease),
  };
}

function expectForegroundLeaseLifecycle(
  beforeRecords: Record<string, string>,
  activeRecords: Record<string, string>,
  afterRecords: Record<string, string>,
): void {
  const before = foregroundNodeLeasePool(beforeRecords);
  const active = foregroundNodeLeasePool(activeRecords);
  const after = foregroundNodeLeasePool(afterRecords);
  expect(before.active).toEqual([]);
  expect(before.reusable).toHaveLength(1);
  expect(active.active).toHaveLength(1);
  expect(active.reusable).toEqual([]);
  expect(active.active[0]!.leaseId).not.toBe(before.reusable[0]!.leaseId);
  expect(active.active[0]!.node).toEqual(before.reusable[0]!.node);
  expect(active.active[0]!.confirmedTxTime).toBe(before.reusable[0]!.confirmedTxTime);
  expect(active.retired).toEqual(before.retired);

  expect(after.active).toEqual([]);
  expect(after.reusable).toHaveLength(1);
  expect(after.reusable[0]!.leaseId).toBe(active.active[0]!.leaseId);
  expect(after.reusable[0]!.node).toEqual(active.active[0]!.node);
  expect(after.reusable[0]!.confirmedTxTime).toBe(active.active[0]!.confirmedTxTime);
  expect(after.retired).toEqual(active.retired);
}

function corruptReusableLeaseHighWater(records: Record<string, string>): Record<string, string> {
  const corrupted = { ...records };
  const manifest = rawManifest(corrupted);
  const entry = manifest.find(([key]) => key === "foreground-node-leases-v1");
  if (!entry || !isRawForegroundNodeLeasePool(entry[1]) || entry[1].reusable.length !== 1) {
    throw new Error("expected one reusable foreground-node lease to corrupt");
  }
  entry[1].reusable[0]!.confirmedTxTime = "0";
  corrupted[INDEXEDDB_STORAGE_MANIFEST_STORE] = JSON.stringify(manifest);
  return corrupted;
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

async function installRawRecords(name: string, records: Record<string, string>): Promise<void> {
  const request = indexedDB.open(name, INDEXEDDB_BTREE_DATABASE_VERSION);
  request.onupgradeneeded = () => {
    const database = request.result;
    database.createObjectStore(INDEXEDDB_BTREE_PAGES_STORE);
    database.createObjectStore(INDEXEDDB_BTREE_METADATA_STORE);
    database.createObjectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
  };
  const database = await requestResult(request);
  const names = [
    INDEXEDDB_BTREE_PAGES_STORE,
    INDEXEDDB_BTREE_METADATA_STORE,
    INDEXEDDB_STORAGE_MANIFEST_STORE,
  ];
  const transaction = database.transaction(names, "readwrite");
  for (const storeName of names) {
    for (const [key, value] of JSON.parse(records[storeName] ?? "[]") as [IDBValidKey, unknown][]) {
      transaction
        .objectStore(storeName)
        .put(
          restoreStructuredCloneValue(
            value,
            storeName === INDEXEDDB_BTREE_PAGES_STORE || key === "replica-node-v1",
          ),
          key,
        );
    }
  }
  await transactionDone(transaction);
  database.close();
}

function restoreStructuredCloneValue(value: unknown, binary = false, property?: string): unknown {
  if (Array.isArray(value)) {
    if (binary || property === "node") return Uint8Array.from(value as number[]).buffer;
    return value.map((nested) => restoreStructuredCloneValue(nested));
  }
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, nested]) => [
      key,
      restoreStructuredCloneValue(nested, false, key),
    ]),
  );
}

function serializeRawRecord(value: unknown): unknown {
  if (value instanceof ArrayBuffer) return Array.from(new Uint8Array(value));
  if (ArrayBuffer.isView(value)) {
    return Array.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
  }
  if (Array.isArray(value)) return value.map(serializeRawRecord);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, nested]) => [key, serializeRawRecord(nested)]),
    );
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
