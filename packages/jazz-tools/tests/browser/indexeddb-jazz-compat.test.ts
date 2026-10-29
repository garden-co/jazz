/**
 * A real-Chromium, public-Jazz compatibility receipt for a durable browser
 * replica. A browser store is two IndexedDB databases: the page store (its
 * epoch manifest, owner, replica node and leases) named by the physical
 * root, and the worker's SQLite file, which the IndexedDB VFS keeps as
 * 64 KiB blocks in the `jazz-idb` database. This file drives both through
 * `createDb`, the SharedWorker and the released WASM runtime.
 *
 * - The pinned SQLite storage-format-1 corpus reopens offline with every
 *   row, branch and large value, accepts current writes, and reopens again.
 * - A damaged SQLite file is refused with a storage error, not rewritten.
 * - Every Groove-era root (published alpha.54, the last Groove format, and
 *   the pre-linear one) is refused with the typed storage-format error,
 *   naming the codec families it declares, without rewriting a record.
 *
 * Keep this corpus on the canonical browser test project. It is intentionally
 * not a second fake-IDB or MemoryPageStore harness: a durable fixture must be
 * readable by the same public path an application uses.
 */

import { afterEach, describe, expect, it } from "vitest";
import historicalCorpus from "../../fixtures/epoch-1-browser-jazz-corpus.json?raw";
import publishedAlpha54Corpus from "../../fixtures/published-alpha54-browser-jazz-corpus.json?raw";
import lastGrooveCorpus from "../../fixtures/groove-linear-history-browser-jazz-corpus.json?raw";
import preLinearCorpus from "../../fixtures/pre-linear-browser-jazz-corpus.json?raw";
import sqliteCorpus from "../../fixtures/sqlite-v1-browser-jazz-corpus.json?raw";
import { jazzStorageCorpusBrowserCommands } from "./browser-commands.js";
import { createAccountManager, ReadTier, schema as s, type DbConfig } from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import { accountRegistryUrl } from "../../src/accounts/context.js";
import { createBrowserPhysicalDatabaseName } from "../../src/runtime/browser-worker-config.js";
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
import { createJazzServerTransportControl, getJazzServerInfo } from "./testing-server.js";
import {
  createBrowserTestDb as createDb,
  sleep,
  TestCleanup,
  uniqueDbName,
  waitForCondition,
  withTimeout,
} from "./support.js";

const app = s.defineApp({
  projects: s.table(
    {
      name: s.string(),
    },
    { documentsViaProject: s.reverse("documents", "project") },
  ),
  documents: s
    .table(
      {
        branch: s.string(),
        title: s.string(),
        projectId: s.uuid(),
        body: s.string(),
      },
      { project: s.rel("projects", "projectId") },
    )
    .branchBy("branch"),
  notes: s
    .table({
      branch: s.string(),
      title: s.string(),
      secret: s.boolean(),
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
  policy.notes.allowRead.where({ secret: false }),
  policy.notes.allowInsert.always(),
  policy.notes.allowUpdate.always(),
  policy.notes.allowDelete.always(),
]);

/** The draft of the notes viewed over main. */
const NOTES_VIEW = { branch: "draft", base: "main" } as const;

// Keep the corrupt-open receipt deliberately small. The broader corpus above
// exercises branches and large values; this isolates storage admission from
// any schema-level query work that cannot run after an invalid epoch anyway.
const corruptionApp = s.defineApp({
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
    },
    {},
  ),
});

/** The producer's account; every pinned corpus was written under it. */
const CORPUS_SECRET = "jazz-auth-v1:AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
const CORPUS_APP_ID = "ba96582c-7167-5f52-ba63-3ebefe1c2b96";
const LARGE_BODY = "large value ".repeat(20_000);

describe("browser Jazz storage compatibility corpus", () => {
  // `driver.dbName` is a caller-selected logical base. Browser persistence
  // deliberately derives a distinct physical root for the complete auth
  // scope, so raw compatibility inspection and cleanup must use the resolved
  // root that the public Db actually opened.
  const databaseNames = new Set<string>();
  const openDbs: Db[] = [];
  const transportCleanup = new TestCleanup();
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
    openDbs.splice(openDbs.indexOf(db), 1);
  }

  afterEach(async () => {
    receipt(
      `cleanup:start; pinned-phase=${pinnedCorpusPhase}; open=${openDbs
        .map((db) => openDbLabels.get(db) ?? "unlabeled")
        .join(",")}`,
    );
    await Promise.all(
      [...openDbs]
        .reverse()
        .map((db) => shutdownTrackedDb(db, openDbLabels.get(db) ?? "unlabeled")),
    );
    await transportCleanup.cleanup();
    receipt(`cleanup:dbs-done; pinned-phase=${pinnedCorpusPhase}`);
    await Promise.all(
      [...databaseNames].map(async (name) => {
        await IndexedDbPageStore.destroy(name);
        await deleteSqliteBlocks(await sqliteFilePath(name));
      }),
    );
    databaseNames.clear();
    receipt(`cleanup:done; pinned-phase=${pinnedCorpusPhase}`);
  });

  it("produces the SQLite catalogue/history/branch/large-value corpus through public WasmDb", async () => {
    const server = await getJazzServerInfo(CORPUS_APP_ID);
    await deploy({
      appId: server.appId,
      serverUrl: server.serverUrl,
      adminSecret: server.adminSecret,
      schema: app.wasmSchema,
      permissions,
    });
    const transport = transportCleanup.trackTransport(
      await createJazzServerTransportControl(server.serverUrl),
    );
    const dbName = uniqueDbName("browser-storage-sqlite-producer");
    const config = await persistentConfig(dbName, CORPUS_SECRET, {
      ...server,
      serverUrl: transport.url,
    });
    let db = await openPersistentDb(config, "producer");
    const physicalDbName = await trackPhysicalDatabase(dbName);
    const initial = await db.transaction((tx) => {
      const project = tx.insert(app.projects, { name: "corpus project" });
      const main = tx.insert(
        app.documents,
        { branch: "main", title: "original title", projectId: project.id, body: LARGE_BODY },
        { branch: "main" },
      );
      tx.insert(
        app.documents,
        { branch: "draft", title: "draft override", projectId: project.id, body: LARGE_BODY },
        { branch: "draft" },
      );
      return main.id;
    });
    await withTimeout(
      initial.wait({ tier: "global" }),
      20_000,
      "corpus initial write did not settle",
    );
    await withTimeout(
      db
        .update(app.documents, initial.value, { title: "current title" }, { branch: "main" })
        .wait({ tier: "global" }),
      20_000,
      "corpus history update did not settle",
    );
    expect(await db.all(app.documents, { tier: "remote", branch: "main" })).toHaveLength(1);
    expect(await db.all(app.documents, { tier: "remote", branch: "draft" })).toHaveLength(1);
    // A note the draft overrides with a version this replica may not read:
    // its view of the draft over main hides the note, and the replica keeps
    // a marker of it (`_jazz_withheld`).
    const note = db.insert(
      app.notes,
      { branch: "main", title: "shared note", secret: false },
      { branch: "main" },
    );
    await withTimeout(note.wait({ tier: "global" }), 20_000, "corpus note did not settle");
    let viewed: { title: string }[] | undefined;
    const unsubscribe = db.subscribe(
      app.notes,
      (rows) => {
        viewed = rows;
      },
      NOTES_VIEW,
    );
    await waitForCondition(async () => viewed?.length === 1, 20_000, "the notes view did not open");
    await withTimeout(
      db.update(app.notes, note.value.id, { secret: true }, NOTES_VIEW).wait({ tier: "global" }),
      20_000,
      "corpus secret note did not settle",
    );
    await waitForCondition(
      async () => viewed?.length === 0,
      20_000,
      "the notes view did not hide the note",
    );
    unsubscribe();
    await shutdownTrackedDb(db, "producer");
    await sleep(100);
    const candidate = await rawBrowserStore(physicalDbName);
    expect(
      rawManifest(candidate).find(([key]) => key === INDEXEDDB_STORAGE_MANIFEST_KEY)?.[1],
    ).toEqual(INDEXEDDB_STORAGE_MANIFEST);
    expect(JSON.parse(candidate[SQLITE_BLOCKS]!)).not.toHaveLength(0);
    // Validate the raw candidate through a real offline reopen before export.
    await transport.block();
    try {
      db = await openPersistentDb(config, "producer-reopen");
      await db.disconnect();
      expect(
        await db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "main" }),
      ).toMatchObject([{ branch: "main", title: "current title", body: LARGE_BODY }]);
      expect(
        await db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "draft" }),
      ).toMatchObject([{ branch: "draft", title: "draft override", body: LARGE_BODY }]);
      expect(await db.all(app.notes, { tier: ReadTier.LocalFirst, ...NOTES_VIEW })).toEqual([]);
      await shutdownTrackedDb(db, "producer-reopen");
    } finally {
      await transport.unblock();
    }
    await jazzStorageCorpusBrowserCommands().writeBrowserStorageCorpus(candidate);
  }, 90_000);

  // `sqlite-v1-browser-jazz-corpus.json` is real output of the producer test
  // above: the page store's records plus the SQLite file's blocks, written in
  // Jazz SQLite storage format 1. Once alpha.60 ships it is immutable;
  // until then a format change re-runs the producer (JAZZ_BROWSER_CORPUS_OUT
  // set to a new path) and checks in the exported candidate.
  it("opens the pinned SQLite corpus offline, accepts current writes and reopens", async () => {
    pinnedCorpusPhase = "pinned-test:start";
    receipt(pinnedCorpusPhase);
    const pinned = JSON.parse(sqliteCorpus) as Record<string, string>;
    const { config, physicalDbName, dbName } = await pinnedCorpusConfig(
      pinned,
      "browser-storage-compat-sqlite-v1",
    );
    await pinnedPhase("install-pinned-records", () => installBrowserStore(physicalDbName, pinned));
    expect(await rawBrowserStore(physicalDbName)).toEqual(pinned);

    let db = await pinnedPhase("readonly-open", () => openPersistentDb(config, "pinned-readonly"));
    expect(await trackPhysicalDatabase(dbName)).toBe(physicalDbName);
    const rawWhileReopened = await rawBrowserStore(physicalDbName);
    // No serverUrl: every read and append is local, so no server can repair
    // a missing page.
    const reopenedMain = await pinnedPhase("readonly-main-query", () =>
      db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "main" }),
    );
    const reopenedDraft = await pinnedPhase("readonly-draft-query", () =>
      db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "draft" }),
    );
    expect(reopenedMain).toMatchObject([
      { branch: "main", title: "current title", body: LARGE_BODY },
    ]);
    expect(reopenedDraft).toMatchObject([
      { branch: "draft", title: "draft override", body: LARGE_BODY },
    ]);
    const historicalProjects = await db.all(app.projects, { tier: ReadTier.LocalFirst });
    expect(historicalProjects).toMatchObject([{ name: "corpus project" }]);
    // The draft's secret version still hides the note in the view.
    expect(await db.all(app.notes, { tier: ReadTier.LocalFirst, ...NOTES_VIEW })).toEqual([]);
    await pinnedPhase("readonly-shutdown", () => shutdownTrackedDb(db, "pinned-readonly"));
    await sleep(100);
    const rawAfterReadOnly = await rawBrowserStore(physicalDbName);
    expectForegroundLeaseLifecycle(pinned, rawWhileReopened, rawAfterReadOnly);
    // Reading changes nothing durable but the opaque lease token.
    expect(normalizeRuntimeLeaseRecords(rawAfterReadOnly)).toEqual(
      normalizeRuntimeLeaseRecords(pinned),
    );
    expect(
      normalizeRuntimeLeaseRecords(corruptReusableLeaseHighWater(rawAfterReadOnly)),
    ).not.toEqual(normalizeRuntimeLeaseRecords(rawAfterReadOnly));

    // Append with today's writer; both generations survive a fresh open.
    db = await pinnedPhase("writer-open", () => openPersistentDb(config, "pinned-writer"));
    expect(
      await db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "main" }),
    ).toMatchObject([{ title: "current title" }]);
    const currentBody = "current writer large value ".repeat(12_000);
    const currentWrite = await pinnedPhase("writer-transaction", () =>
      db.transaction((tx) => {
        const project = tx.insert(app.projects, { name: "current writer project" });
        tx.insert(
          app.documents,
          {
            branch: "main",
            title: "current writer document",
            projectId: project.id,
            body: currentBody,
          },
          { branch: "main" },
        );
        tx.update(
          app.documents,
          reopenedMain[0]!.id,
          { title: "reopened writer" },
          { branch: "main" },
        );
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
    await sleep(100);
    const rawAfterWrite = await rawBrowserStore(physicalDbName);
    expect(rawAfterWrite[SQLITE_BLOCKS]).not.toEqual(pinned[SQLITE_BLOCKS]);

    db = await pinnedPhase("offline-open", () => openPersistentDb(config, "pinned-offline"));
    const mixedMain = await db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "main" });
    expect(mixedMain).toHaveLength(2);
    expect(mixedMain).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ title: "reopened writer", body: LARGE_BODY }),
        expect.objectContaining({ title: "current writer document", body: currentBody }),
      ]),
    );
    expect(await db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "draft" })).toEqual(
      reopenedDraft,
    );
    const mixedProjects = await db.all(app.projects, { tier: ReadTier.LocalFirst });
    expect(mixedProjects).toHaveLength(2);
    expect(mixedProjects).toEqual(expect.arrayContaining(historicalProjects));
    pinnedCorpusPhase = "pinned-test:complete";
    receipt(pinnedCorpusPhase);
  }, 90_000);

  // Damage in the SQLite file is a storage error, never rows read from a
  // corrupt page, and the refused open rewrites nothing.
  it("refuses the pinned SQLite corpus with a damaged file header without rewriting it", async () => {
    const pinned = JSON.parse(sqliteCorpus) as Record<string, string>;
    const blocks = JSON.parse(pinned[SQLITE_BLOCKS]!) as [number, string][];
    const header = blocks.find(([offset]) => offset === 0)!;
    const damagedHeader = base64ToBytes(header[1]);
    // "SQLite format 3\0" becomes something SQLite does not open.
    damagedHeader.fill(0x55, 0, 16);
    const damaged = {
      ...pinned,
      [SQLITE_BLOCKS]: JSON.stringify(
        blocks.map(([offset, data]) =>
          offset === 0 ? [offset, bytesToBase64(damagedHeader)] : [offset, data],
        ),
      ),
    };
    const { config, physicalDbName } = await pinnedCorpusConfig(
      pinned,
      "browser-storage-compat-sqlite-v1-damaged",
    );
    await installBrowserStore(physicalDbName, damaged);
    // The worker opens the file with the first schema it serves, so the
    // first read is where the damage surfaces.
    const db = await openPersistentDb(config, "damaged");
    await expect(
      withTimeout(
        db.all(app.documents, { tier: ReadTier.LocalFirst, branch: "main" }),
        5_000,
        "damaged SQLite read did not reject",
      ),
    ).rejects.toThrow("Storage: file is not a database");
    await shutdownTrackedDb(db, "damaged");
    await sleep(100);
    expect((await rawBrowserStore(physicalDbName))[SQLITE_BLOCKS]).toEqual(damaged[SQLITE_BLOCKS]);
  }, 30_000);

  // Every Groove-era browser root declares Groove's codec families. The
  // SQLite build reads none of them: opening is refused at manifest
  // admission with the typed storage-format error, before the worker opens a
  // SQLite file, and no record is rewritten.
  for (const [label, corpus, digest] of [
    [
      "the published alpha.54 corpus",
      publishedAlpha54Corpus,
      "e1aa237f375db4d060c2a3fe13fa443659fba55c8695f99993e17e200c897b00",
    ],
    ["the last Groove (linear-history) corpus", lastGrooveCorpus, undefined],
    ["the pre-linear corpus", preLinearCorpus, undefined],
  ] as const) {
    it(`refuses ${label} with a typed storage-format error`, async () => {
      if (digest) expect(await sha256Hex(corpus)).toBe(digest);
      const pinned = JSON.parse(corpus) as Record<string, string>;
      const declared = (
        rawManifest(pinned).find(([key]) => key === INDEXEDDB_STORAGE_MANIFEST_KEY)![1] as {
          requiredCodecIds: string[];
        }
      ).requiredCodecIds;
      expect(declared.some((codec) => codec.startsWith("groove."))).toBe(true);
      const { config, physicalDbName } = await pinnedCorpusConfig(
        pinned,
        uniqueDbName("browser-storage-compat-groove"),
      );
      await installRawRecords(physicalDbName, pinned);
      expect(await rawRecords(physicalDbName)).toEqual(pinned);
      await expect(
        withTimeout(createDb(config), 5_000, `${label} open did not reject`),
      ).rejects.toThrow(
        `unsupported storage format: this epoch-1 root lacks codec families ["jazz.sqlite-storage.v1"] required by this build and declares ${JSON.stringify(declared)} that this build does not read`,
      );
      expect(await rawRecords(physicalDbName)).toEqual(pinned);
      expect(JSON.parse((await rawBrowserStore(physicalDbName))[SQLITE_BLOCKS]!)).toEqual([]);
    }, 30_000);
  }

  it("rejects the historical retired-result codec profile without rewriting its pages", async () => {
    const server = await getJazzServerInfo(CORPUS_APP_ID);
    const dbName = uniqueDbName("browser-storage-retired-profile");
    const config = await persistentConfig(dbName, CORPUS_SECRET, server);
    const bootstrap = await openPersistentDb(config, "retired-bootstrap");
    const physicalDbName = await trackPhysicalDatabase(dbName);
    await shutdownTrackedDb(bootstrap, "retired-bootstrap");
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
    const dbName = uniqueDbName("jazz-storage-compat");
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
    const rawBeforeRejectedRead = await rawBrowserStore(physicalDbName);
    // Schema selection is lazy, but persistent construction must first obtain
    // a foreground-node lease. Its worker opens the physical root before it
    // can mint a transaction identity, making createDb the durable admission
    // boundary. Reject the exact original epoch error rather than leaking a
    // worker error or returning a handle backed by corrupt state.
    try {
      await expect(
        withTimeout(createDb(config), 5_000, "corrupt epoch open did not reject"),
      ).rejects.toThrow("Missing or invalid IndexedDB storage epoch manifest");
      expect(await rawBrowserStore(physicalDbName)).toEqual(rawBeforeRejectedRead);
    } finally {
      await cleanup.cleanup();
    }
  }, 90_000);

  /**
   * A config for the pinned corpus's owner under a fresh logical base, and
   * the physical root it resolves to. Registry authority is part of the
   * stored owner, so the account is restored under the producer's
   * authority, even though that test server no longer exists. The config
   * has no serverUrl: nothing upstream can repair a pinned page. The base is
   * new per test, so its SQLite file has never been opened by this worker.
   */
  async function pinnedCorpusConfig(
    records: Record<string, string>,
    dbName: string,
  ): Promise<{ config: DbConfig; physicalDbName: string; dbName: string }> {
    const owner = JSON.parse(
      rawManifest(records).find(
        ([key]) => key === INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY,
      )![1] as string,
    );
    expect(owner).toMatchObject({ version: 1, env: "dev", auth: { kind: "account" } });
    const registry = new URL(owner.auth.registry);
    const registrySuffix = `/apps/${owner.appId}/accounts`;
    expect(registry.pathname.endsWith(registrySuffix)).toBe(true);
    const serverBase = new URL(registry);
    serverBase.pathname = registry.pathname.slice(0, -registrySuffix.length);
    expect(accountRegistryUrl(serverBase.href, owner.appId)).toBe(owner.auth.registry);
    const accounts = await createAccountManager({
      appId: owner.appId,
      serverUrl: serverBase.href,
    });
    const account = accounts.restoreLocalFirst(CORPUS_SECRET);
    expect(account.id).toBe(owner.auth.account);
    const base = uniqueDbName(dbName);
    const config: DbConfig = {
      appId: owner.appId,
      account,
      driver: { type: "persistent", dbName: base },
    };
    // The root is named by the scope createDb resolves the account to,
    // which the owner record stores.
    const physicalDbName = createBrowserPhysicalDatabaseName(
      { ...config, accountId: owner.auth.account, accountRegistryAuthority: owner.auth.registry },
      base,
    );
    databaseNames.add(physicalDbName);
    return { config, physicalDbName, dbName: base };
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

// ---- the SQLite file on the IndexedDB VFS -----------------------------------

/** Where the IndexedDB VFS (`jazz-wasm`'s `jazz-idb`) keeps file blocks. */
const SQLITE_VFS_DATABASE = "jazz-idb";
const SQLITE_VFS_STORE = "blocks";
/** The corpus key holding the SQLite file's blocks, as `[offset, base64]`. */
const SQLITE_BLOCKS = "jazz-idb/blocks";

/**
 * The SQLite file the worker keeps for a physical root: named by a hash of
 * the root's name (see `StoreSpec::from_js` in crates/jazz-wasm/src/browser.rs).
 */
async function sqliteFilePath(physicalDbName: string): Promise<string> {
  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", new TextEncoder().encode(physicalDbName)),
  );
  return `/jazz-${Array.from(digest.slice(0, 16), (byte) => byte.toString(16).padStart(2, "0")).join("")}.sqlite`;
}

function openSqliteVfsDatabase(): Promise<IDBDatabase> {
  const request = indexedDB.open(SQLITE_VFS_DATABASE, 1);
  request.onupgradeneeded = () => {
    request.result.createObjectStore(SQLITE_VFS_STORE, { keyPath: ["path", "offset"] });
  };
  return requestResult(request);
}

function fileBlocks(path: string): IDBKeyRange {
  return IDBKeyRange.bound([path, 0], [path, Number.POSITIVE_INFINITY]);
}

async function rawSqliteBlocks(path: string): Promise<string> {
  const database = await openSqliteVfsDatabase();
  const transaction = database.transaction(SQLITE_VFS_STORE, "readonly");
  const blocks = (await requestResult(
    transaction.objectStore(SQLITE_VFS_STORE).getAll(fileBlocks(path)),
  )) as { offset: number; data: Uint8Array }[];
  await transactionDone(transaction);
  database.close();
  return JSON.stringify(
    blocks
      .map((block) => [block.offset, bytesToBase64(new Uint8Array(block.data))] as const)
      .sort(([left], [right]) => left - right),
  );
}

async function installSqliteBlocks(path: string, blocks: string): Promise<void> {
  const database = await openSqliteVfsDatabase();
  const transaction = database.transaction(SQLITE_VFS_STORE, "readwrite");
  const store = transaction.objectStore(SQLITE_VFS_STORE);
  store.delete(fileBlocks(path));
  for (const [offset, data] of JSON.parse(blocks) as [number, string][]) {
    store.put({ path, offset, data: base64ToBytes(data) });
  }
  await transactionDone(transaction);
  database.close();
}

async function deleteSqliteBlocks(path: string): Promise<void> {
  await installSqliteBlocks(path, "[]");
}

/** The page store's records plus the SQLite file's blocks. */
async function rawBrowserStore(physicalDbName: string): Promise<Record<string, string>> {
  return {
    ...(await rawRecords(physicalDbName)),
    [SQLITE_BLOCKS]: await rawSqliteBlocks(await sqliteFilePath(physicalDbName)),
  };
}

async function installBrowserStore(
  physicalDbName: string,
  records: Record<string, string>,
): Promise<void> {
  const { [SQLITE_BLOCKS]: blocks = "[]", ...pageStore } = records;
  await installRawRecords(physicalDbName, pageStore);
  await installSqliteBlocks(await sqliteFilePath(physicalDbName), blocks);
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let index = 0; index < bytes.length; index += 0x8000) {
    binary += String.fromCharCode(...bytes.subarray(index, index + 0x8000));
  }
  return btoa(binary);
}

function base64ToBytes(base64: string): Uint8Array {
  return Uint8Array.from(atob(base64), (char) => char.charCodeAt(0));
}

async function sha256Hex(text: string): Promise<string> {
  return Array.from(
    new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text))),
  )
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

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
