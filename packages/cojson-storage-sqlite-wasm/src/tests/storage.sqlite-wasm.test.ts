import { LocalNode, StorageApiAsync, cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import { getSqliteWasmStorage } from "../index.js";
import { SqliteWasmDriver } from "../SqliteWasmDriver.js";
import { toSimplifiedMessages } from "./messagesTestUtils.js";
import {
  connectToSyncServer,
  createTestNode,
  getAllCoValuesWaitingForDelete,
  getCoValueStoredSessions,
  trackMessages,
  waitFor,
} from "./testUtils.js";

const Crypto = await WasmCrypto.create();

// Helper: create a shared in-memory driver instance.
// Each call to `getSqliteWasmStorage(driver)` creates a new `StorageApiAsync`
// wrapper over the same underlying DB — mirroring how IndexedDB tests work
// (each `getIndexedDBStorage()` creates a fresh wrapper over the same IDB).
async function createSharedDriver() {
  const driver = new SqliteWasmDriver("test.db", false);
  await driver.initialize();
  return driver;
}

async function createInMemoryStorage(driver?: SqliteWasmDriver) {
  if (driver) {
    return getSqliteWasmStorage(driver);
  }
  return getSqliteWasmStorage("test.db", false);
}

// ─── Driver Unit Tests ──────────────────────────────────────────────────────

describe("SqliteWasmDriver", () => {
  let driver: SqliteWasmDriver;

  beforeEach(async () => {
    driver = new SqliteWasmDriver("test.db", false);
    await driver.initialize();
  });

  afterEach(async () => {
    await driver.closeDb();
  });

  test("should execute DDL and query", async () => {
    await driver.run(
      "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)",
      [],
    );
    await driver.run("INSERT INTO test (name) VALUES (?)", ["Alice"]);

    const rows = await driver.query<{ id: number; name: string }>(
      "SELECT * FROM test",
      [],
    );
    expect(rows).toEqual([{ id: 1, name: "Alice" }]);
  });

  test("should return single row with get()", async () => {
    await driver.run("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)", []);
    await driver.run("INSERT INTO kv VALUES (?, ?)", ["key1", "val1"]);

    const row = await driver.get<{ k: string; v: string }>(
      "SELECT * FROM kv WHERE k = ?",
      ["key1"],
    );
    expect(row).toEqual({ k: "key1", v: "val1" });
  });

  test("should return undefined for missing row", async () => {
    await driver.run("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)", []);

    const row = await driver.get<{ k: string; v: string }>(
      "SELECT * FROM kv WHERE k = ?",
      ["missing"],
    );
    expect(row).toBeUndefined();
  });

  test("should commit transactions", async () => {
    await driver.run("CREATE TABLE t (v INTEGER)", []);

    await driver.transaction(async (tx) => {
      await tx.run("INSERT INTO t VALUES (?)", [1]);
      await tx.run("INSERT INTO t VALUES (?)", [2]);
    });

    const rows = await driver.query<{ v: number }>(
      "SELECT v FROM t ORDER BY v",
      [],
    );
    expect(rows).toEqual([{ v: 1 }, { v: 2 }]);
  });

  test("should rollback failed transactions", async () => {
    await driver.run("CREATE TABLE t (v INTEGER)", []);

    await expect(
      driver.transaction(async (tx) => {
        await tx.run("INSERT INTO t VALUES (?)", [1]);
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    const rows = await driver.query<{ c: number }>(
      "SELECT COUNT(*) as c FROM t",
      [],
    );
    expect(rows[0]!.c).toBe(0);
  });

  test("should read and write migration version", async () => {
    expect(await driver.getMigrationVersion()).toBe(0);
    await driver.saveMigrationVersion(5);
    expect(await driver.getMigrationVersion()).toBe(5);
  });
});

// ─── Storage Integration Tests ──────────────────────────────────────────────

let syncMessages: ReturnType<typeof trackMessages>;

beforeEach(() => {
  syncMessages = trackMessages();
  cojsonInternals.setSyncStateTrackingBatchDelay(0);
  cojsonInternals.setCoValueLoadingRetryDelay(10);
});

afterEach(async () => {
  syncMessages.restore();
});

test("should sync and load data from storage", async () => {
  const storage = await createInMemoryStorage();

  const node1 = createTestNode();
  node1.setStorage(storage);

  const group = node1.createGroup();
  const map = group.createMap();

  map.set("hello", "world");

  await map.core.waitForSync();

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> CONTENT Group header: true new: After: 0 New: 4",
      "client -> CONTENT Map header: true new: After: 0 New: 1",
    ]
  `);

  node1.gracefulShutdown();
  syncMessages.clear();

  const node2 = createTestNode({ secret: node1.agentSecret });
  node2.setStorage(storage);

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map2.get("hello")).toBe("world");

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Map sessions: empty",
      "storage -> CONTENT Group header: true new: After: 0 New: 4",
      "storage -> CONTENT Map header: true new: After: 0 New: 1",
    ]
  `);
});

test("should send an empty content message if there is no content", async () => {
  const storage = await createInMemoryStorage();

  const node1 = createTestNode();
  node1.setStorage(storage);

  const group = node1.createGroup();
  const map = group.createMap();

  await map.core.waitForSync();

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> CONTENT Group header: true new: After: 0 New: 4",
      "client -> CONTENT Map header: true new: ",
    ]
  `);

  syncMessages.clear();
  node1.gracefulShutdown();

  const node2 = createTestNode({ secret: node1.agentSecret });
  node2.setStorage(storage);

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Map sessions: empty",
      "storage -> CONTENT Group header: true new: After: 0 New: 4",
      "storage -> CONTENT Map header: true new: ",
    ]
  `);
});

test("persists deleted coValue marker as a deletedCoValues work queue entry", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );
  const storage = await createInMemoryStorage();
  node.setStorage(storage);

  const group = node.createGroup();
  const map = group.createMap();
  map.set("hello", "world");

  const map2 = group.createMap();
  map2.set("hello2", "world2");

  await map.core.waitForSync();
  await map2.core.waitForSync();

  map.core.deleteCoValue();
  map2.core.deleteCoValue();

  await map.core.waitForSync();
  await map2.core.waitForSync();

  const deletedCoValueIDs = await getAllCoValuesWaitingForDelete(storage);
  expect(deletedCoValueIDs).toContain(map.id);
  expect(deletedCoValueIDs).toContain(map2.id);
});

test("delete flow: eraseAllDeletedCoValues removes history, preserves tombstone", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const storage = await createInMemoryStorage();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );
  node1.setStorage(storage);

  const group = node1.createGroup();
  const map = group.createMap();
  map.set("hello", "world");
  await map.core.waitForSync();

  map.core.deleteCoValue();
  await map.core.waitForSync();

  await waitFor(async () => {
    const queued = await getAllCoValuesWaitingForDelete(storage);
    expect(queued).toContain(map.id);
    return true;
  });

  await storage.eraseAllDeletedCoValues();

  // Queue drained
  await waitFor(async () => {
    const queued = await getAllCoValuesWaitingForDelete(storage);
    expect(queued).not.toContain(map.id);
    return true;
  });

  // Tombstone-only load from storage
  node1.gracefulShutdown();
  syncMessages.clear();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );
  node2.setStorage(storage);

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map2.core.isDeleted).toBe(true);
  expect(map2.get("hello")).toBeUndefined();

  const sessionIDs = await getCoValueStoredSessions(storage, map.id);
  expect(sessionIDs).toHaveLength(1);
  expect(sessionIDs[0]).toMatch(/_session_d[1-9A-HJ-NP-Za-km-z]+\$$/);
});

test("should load dependencies correctly (group inheritance)", async () => {
  const storage = await createInMemoryStorage();

  const node1 = createTestNode();
  node1.setStorage(storage);
  const group = node1.createGroup();
  const parentGroup = node1.createGroup();

  group.extend(parentGroup);

  const map = group.createMap();

  map.set("hello", "world");

  await map.core.waitForSync();

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
        ParentGroup: parentGroup.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> CONTENT Group header: true new: After: 0 New: 4",
      "client -> CONTENT ParentGroup header: true new: After: 0 New: 4",
      "client -> CONTENT Group header: false new: After: 4 New: 2",
      "client -> CONTENT Map header: true new: After: 0 New: 1",
    ]
  `);

  syncMessages.clear();
  node1.gracefulShutdown();

  const node2 = createTestNode({ secret: node1.agentSecret });
  node2.setStorage(storage);

  await node2.load(map.id);

  expect(node2.expectCoValueLoaded(map.id)).toBeTruthy();
  expect(node2.expectCoValueLoaded(group.id)).toBeTruthy();
  expect(node2.expectCoValueLoaded(parentGroup.id)).toBeTruthy();

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
        ParentGroup: parentGroup.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Map sessions: empty",
      "storage -> CONTENT ParentGroup header: true new: After: 0 New: 4",
      "storage -> CONTENT Group header: true new: After: 0 New: 6",
      "storage -> CONTENT Map header: true new: After: 0 New: 1",
    ]
  `);
});

test("should not send the same dependency value twice", async () => {
  const storage = await createInMemoryStorage();

  const node1 = createTestNode();
  node1.setStorage(storage);

  const group = node1.createGroup();
  const parentGroup = node1.createGroup();

  group.extend(parentGroup);

  const mapFromParent = parentGroup.createMap();
  const map = group.createMap();

  map.set("hello", "world");
  mapFromParent.set("hello", "world");

  await map.core.waitForSync();
  await mapFromParent.core.waitForSync();

  syncMessages.clear();
  node1.gracefulShutdown();

  const node2 = createTestNode({ secret: node1.agentSecret });
  node2.setStorage(storage);

  await node2.load(map.id);
  await node2.load(mapFromParent.id);

  expect(node2.expectCoValueLoaded(map.id)).toBeTruthy();
  expect(node2.expectCoValueLoaded(mapFromParent.id)).toBeTruthy();
  expect(node2.expectCoValueLoaded(group.id)).toBeTruthy();
  expect(node2.expectCoValueLoaded(parentGroup.id)).toBeTruthy();

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
        ParentGroup: parentGroup.core,
        MapFromParent: mapFromParent.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Map sessions: empty",
      "storage -> CONTENT ParentGroup header: true new: After: 0 New: 4",
      "storage -> CONTENT Group header: true new: After: 0 New: 6",
      "storage -> CONTENT Map header: true new: After: 0 New: 1",
      "client -> LOAD MapFromParent sessions: empty",
      "storage -> CONTENT MapFromParent header: true new: After: 0 New: 1",
    ]
  `);
});

test("should recover from data loss", async () => {
  const storage = await createInMemoryStorage();

  const node1 = createTestNode();
  node1.setStorage(storage);

  const group = node1.createGroup();

  const map = group.createMap();

  map.set("0", 0);

  await map.core.waitForSync();

  const mock = vi
    .spyOn(StorageApiAsync.prototype, "store")
    .mockImplementation(() => Promise.resolve(undefined));

  map.set("1", 1);
  map.set("2", 2);

  await new Promise((resolve) => setTimeout(resolve, 200));

  const knownState = storage.getKnownState(map.id);
  Object.assign(knownState, map.core.knownState());

  mock.mockReset();

  map.set("3", 3);

  await map.core.waitForSync();

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> CONTENT Group header: true new: After: 0 New: 4",
      "client -> CONTENT Map header: true new: After: 0 New: 1",
      "client -> CONTENT Map header: false new: After: 3 New: 1",
      "storage -> KNOWN CORRECTION Map sessions: header/4",
      "client -> CONTENT Map header: false new: After: 1 New: 3",
    ]
  `);

  syncMessages.clear();
  node1.gracefulShutdown();

  const node2 = createTestNode({ secret: node1.agentSecret });
  node2.setStorage(storage);

  const map2 = await node2.load(map.id);

  if (map2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map2.toJSON()).toEqual({
    "0": 0,
    "1": 1,
    "2": 2,
    "3": 3,
  });

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Map sessions: empty",
      "storage -> CONTENT Group header: true new: After: 0 New: 4",
      "storage -> CONTENT Map header: true new: After: 0 New: 4",
    ]
  `);
});

test("should sync multiple sessions in a single content message", async () => {
  const driver = await createSharedDriver();

  const node1 = createTestNode();
  node1.setStorage(await createInMemoryStorage(driver));

  const group = node1.createGroup();

  const map = group.createMap();

  map.set("hello", "world");

  await map.core.waitForSync();

  node1.gracefulShutdown();

  const node2 = createTestNode({ secret: node1.agentSecret });
  node2.setStorage(await createInMemoryStorage(driver));

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map2.get("hello")).toBe("world");

  map2.set("hello", "world2");

  await map2.core.waitForSync();

  node2.gracefulShutdown();

  const node3 = createTestNode({ secret: node1.agentSecret });

  syncMessages.clear();

  node3.setStorage(await createInMemoryStorage(driver));

  const map3 = await node3.load(map.id);
  if (map3 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map3.get("hello")).toBe("world2");

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Map sessions: empty",
      "storage -> CONTENT Group header: true new: After: 0 New: 4",
      "storage -> CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
    ]
  `);
});

test("large coValue upload streaming", async () => {
  const storage = await createInMemoryStorage();

  const node1 = createTestNode();
  node1.setStorage(storage);

  const group = node1.createGroup();
  const largeMap = group.createMap();

  // Generate a large amount of data (about 200KB)
  const dataSize = 1 * 1024 * 200;
  const chunkSize = 1024; // 1KB chunks
  const chunks = dataSize / chunkSize;

  const value = "a".repeat(chunkSize);

  for (let i = 0; i < chunks; i++) {
    const key = `key${i}`;
    largeMap.set(key, value, "trusting");
  }

  await largeMap.core.waitForSync();

  const knownState = largeMap.core.knownState();

  node1.gracefulShutdown();

  const node2 = createTestNode({ secret: node1.agentSecret });

  syncMessages.clear();

  node2.setStorage(storage);

  const largeMapOnNode2 = await node2.load(largeMap.id);

  if (largeMapOnNode2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  await waitFor(() => {
    expect(largeMapOnNode2.core.knownState()).toEqual(knownState);

    return true;
  });

  expect(
    toSimplifiedMessages(
      {
        Map: largeMap.core,
        Group: group.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Map sessions: empty",
      "storage -> CONTENT Group header: true new: After: 0 New: 4",
      "storage -> CONTENT Map header: true new: After: 0 New: 97",
      "storage -> CONTENT Map header: true new: After: 97 New: 97",
      "storage -> CONTENT Map header: true new: After: 194 New: 6",
    ]
  `);
});

test("should sync and load accounts from storage", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();
  const storage = await createInMemoryStorage();

  const { node: node1, accountID } = await LocalNode.withNewlyCreatedAccount({
    crypto: Crypto,
    initialAgentSecret: agentSecret,
    storage,
    creationProps: {
      name: "test",
    },
  });

  const account1 = node1.getCoValue(accountID);
  const profile = node1.expectProfileLoaded(accountID);
  const profileGroup = profile.group;

  expect(
    toSimplifiedMessages(
      {
        Account: account1,
        Profile: profile.core,
        ProfileGroup: profileGroup.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> CONTENT Account header: true new: After: 0 New: 3",
      "client -> CONTENT ProfileGroup header: true new: After: 0 New: 6",
      "client -> CONTENT Profile header: true new: After: 0 New: 1",
      "client -> CONTENT Account header: false new: After: 3 New: 1",
    ]
  `);

  node1.gracefulShutdown();
  syncMessages.restore();
  syncMessages = trackMessages();

  const node2 = await LocalNode.withLoadedAccount({
    crypto: Crypto,
    accountSecret: agentSecret,
    accountID,
    peers: [],
    storage,
    sessionID: Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
  });

  expect(
    toSimplifiedMessages(
      {
        Account: account1,
        Profile: profile.core,
        ProfileGroup: profileGroup.core,
      },
      syncMessages.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Account sessions: empty",
      "storage -> CONTENT Account header: true new: After: 0 New: 4",
      "client -> LOAD Profile sessions: empty",
      "storage -> CONTENT ProfileGroup header: true new: After: 0 New: 6",
      "storage -> CONTENT Profile header: true new: After: 0 New: 1",
    ]
  `);

  expect(node2.getCoValue(accountID).isAvailable()).toBeTruthy();
});

describe("sync state persistence", () => {
  test("unsynced coValues are asynchronously persisted to storage", async () => {
    const client = createTestNode();
    const storage = await createInMemoryStorage();
    client.setStorage(storage);

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Wait for the unsynced coValues to be persisted to storage
    await new Promise<void>((resolve) => setTimeout(resolve, 500));

    const unsyncedCoValueIDs = await new Promise((resolve) =>
      client.storage?.getUnsyncedCoValueIDs(resolve),
    );
    expect(unsyncedCoValueIDs).toHaveLength(2);
    expect(unsyncedCoValueIDs).toContain(map.id);
    expect(unsyncedCoValueIDs).toContain(group.id);

    await client.gracefulShutdown();
  });

  test("synced coValues are removed from storage", async () => {
    const syncServer = createTestNode();
    const client = createTestNode();
    const storage = await createInMemoryStorage();
    client.setStorage(storage);

    connectToSyncServer(client, syncServer);

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Wait enough time for the coValue to be synced
    await new Promise<void>((resolve) => setTimeout(resolve, 500));

    const unsyncedCoValueIDs = await new Promise((resolve) =>
      client.storage?.getUnsyncedCoValueIDs(resolve),
    );
    expect(unsyncedCoValueIDs).toHaveLength(0);
    expect(client.syncManager.unsyncedTracker.has(map.id)).toBe(false);

    await client.gracefulShutdown();
    await syncServer.gracefulShutdown();
  });

  test("unsynced coValues are persisted to storage when the node is shutdown", async () => {
    const client = createTestNode();
    const storage = await createInMemoryStorage();
    client.setStorage(storage);

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Wait for local transaction to trigger sync
    await new Promise<void>((resolve) => queueMicrotask(resolve));

    await client.gracefulShutdown();

    const unsyncedCoValueIDs = await new Promise((resolve) =>
      client.storage?.getUnsyncedCoValueIDs(resolve),
    );
    expect(unsyncedCoValueIDs).toHaveLength(2);
    expect(unsyncedCoValueIDs).toContain(map.id);
    expect(unsyncedCoValueIDs).toContain(group.id);
  });
});

describe("sync resumption", () => {
  test("unsynced coValues are resumed when the node is restarted", async () => {
    const node1 = createTestNode();
    const storage = await createInMemoryStorage();
    node1.setStorage(storage);

    const getUnsyncedCoValueIDsFromStorage = async () =>
      new Promise<string[]>((resolve) =>
        node1.storage?.getUnsyncedCoValueIDs(resolve),
      );

    const group = node1.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    // Wait for the unsynced coValues to be persisted to storage
    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    const unsyncedTracker = node1.syncManager.unsyncedTracker;
    expect(unsyncedTracker.has(map.id)).toBe(true);
    expect(await getUnsyncedCoValueIDsFromStorage()).toHaveLength(2);

    node1.gracefulShutdown();

    // Create second node with the same storage
    const node2 = createTestNode();
    node2.setStorage(storage);

    // Connect to sync server
    const syncServer = createTestNode();
    connectToSyncServer(node2, syncServer);

    await node2.syncManager.waitForAllCoValuesSync();
    // Wait for sync to resume & complete
    await waitFor(
      async () => (await getUnsyncedCoValueIDsFromStorage()).length === 0,
    );

    await node2.gracefulShutdown();
  });
});
