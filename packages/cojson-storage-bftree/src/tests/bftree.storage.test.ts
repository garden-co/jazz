import { LocalNode, cojsonInternals } from "cojson";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import {
  Crypto,
  connectToSyncServer,
  createBfTreeBackend,
  createInMemoryBfTreeStorage,
  createStorageFromBackend,
  createTestNode,
  getAllCoValuesWaitingForDelete,
  getCoValueStoredSessions,
  trackMessages,
  waitFor,
} from "./testUtils.js";

let syncMessages: ReturnType<typeof trackMessages>;

beforeEach(() => {
  syncMessages = trackMessages();
  cojsonInternals.setSyncStateTrackingBatchDelay(0);
  cojsonInternals.setCoValueLoadingRetryDelay(10);
});

afterEach(() => {
  syncMessages.restore();
});

test("store and load round-trip", async () => {
  const backend = createBfTreeBackend();

  const node1 = createTestNode();
  node1.setStorage(createStorageFromBackend(backend));

  const group = node1.createGroup();
  const map = group.createMap();
  map.set("hello", "world");

  await map.core.waitForSync();
  node1.gracefulShutdown();
  syncMessages.clear();

  // New node, same agent, fresh StorageApiAsync wrapping the same backend
  const node2 = createTestNode({ secret: node1.agentSecret });
  node2.setStorage(createStorageFromBackend(backend));

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map2.get("hello")).toBe("world");
});

test("upsertCoValue stores and retrieves header", async () => {
  const node = createTestNode();
  const storage = createInMemoryBfTreeStorage();
  node.setStorage(storage);

  const group = node.createGroup();
  const map = group.createMap();
  map.set("key", "value");

  await map.core.waitForSync();

  const knownState = await new Promise<unknown>((resolve) => {
    storage.loadKnownState(map.id, resolve);
  });

  expect(knownState).toBeDefined();
  expect((knownState as { header: boolean }).header).toBe(true);
});

test("persists deleted coValue marker", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();
  const node = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );
  const storage = createInMemoryBfTreeStorage();
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
  const backend = createBfTreeBackend();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );
  const storage1 = createStorageFromBackend(backend);
  node1.setStorage(storage1);

  const group = node1.createGroup();
  const map = group.createMap();
  map.set("hello", "world");
  await map.core.waitForSync();

  map.core.deleteCoValue();
  await map.core.waitForSync();

  await waitFor(async () => {
    const queued = await getAllCoValuesWaitingForDelete(storage1);
    expect(queued).toContain(map.id);
    return true;
  });

  await storage1.eraseAllDeletedCoValues();

  // Queue drained
  await waitFor(async () => {
    const queued = await getAllCoValuesWaitingForDelete(storage1);
    expect(queued).not.toContain(map.id);
    return true;
  });

  // Tombstone-only load: new node, fresh StorageApiAsync, same backend
  node1.gracefulShutdown();
  syncMessages.clear();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );
  const storage2 = createStorageFromBackend(backend);
  node2.setStorage(storage2);

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map2.core.isDeleted).toBe(true);
  expect(map2.get("hello")).toBeUndefined();

  const sessionIDs = await getCoValueStoredSessions(storage2, map.id);
  expect(sessionIDs).toHaveLength(1);
  expect(sessionIDs[0]).toMatch(/_session_d[1-9A-HJ-NP-Za-km-z]+\$$/);
});

test("should load dependencies correctly (group inheritance)", async () => {
  const backend = createBfTreeBackend();

  const node1 = createTestNode();
  node1.setStorage(createStorageFromBackend(backend));

  const group = node1.createGroup();
  const parentGroup = node1.createGroup();
  group.extend(parentGroup);

  const map = group.createMap();
  map.set("hello", "world");

  await map.core.waitForSync();
  node1.gracefulShutdown();
  syncMessages.clear();

  const node2 = createTestNode({ secret: node1.agentSecret });
  node2.setStorage(createStorageFromBackend(backend));

  await node2.load(map.id);

  expect(node2.expectCoValueLoaded(map.id)).toBeTruthy();
  expect(node2.expectCoValueLoaded(group.id)).toBeTruthy();
  expect(node2.expectCoValueLoaded(parentGroup.id)).toBeTruthy();
});

test("should sync multiple sessions in a single content message", async () => {
  const backend = createBfTreeBackend();

  const node1 = createTestNode();
  node1.setStorage(createStorageFromBackend(backend));

  const group = node1.createGroup();
  const map = group.createMap();
  map.set("hello", "world");

  await map.core.waitForSync();
  node1.gracefulShutdown();

  const node2 = createTestNode({ secret: node1.agentSecret });
  node2.setStorage(createStorageFromBackend(backend));

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map2.get("hello")).toBe("world");

  map2.set("hello", "world2");
  await map2.core.waitForSync();
  node2.gracefulShutdown();
  syncMessages.clear();

  const node3 = createTestNode({ secret: node1.agentSecret });
  node3.setStorage(createStorageFromBackend(backend));

  const map3 = await node3.load(map.id);
  if (map3 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map3.get("hello")).toBe("world2");
});

describe("sync state persistence", () => {
  test("unsynced coValues are asynchronously persisted to storage", async () => {
    const client = createTestNode();
    client.setStorage(createInMemoryBfTreeStorage());

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

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
    client.setStorage(createInMemoryBfTreeStorage());
    connectToSyncServer(client, syncServer);

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    await new Promise<void>((resolve) => setTimeout(resolve, 500));

    const unsyncedCoValueIDs = await new Promise((resolve) =>
      client.storage?.getUnsyncedCoValueIDs(resolve),
    );
    expect(unsyncedCoValueIDs).toHaveLength(0);
    expect(client.syncManager.unsyncedTracker.has(map.id)).toBe(false);

    await client.gracefulShutdown();
    await syncServer.gracefulShutdown();
  });

  test("unsynced coValues are persisted on graceful shutdown", async () => {
    const client = createTestNode();
    client.setStorage(createInMemoryBfTreeStorage());

    const group = client.createGroup();
    const map = group.createMap();
    map.set("key", "value");

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
    const backend = createBfTreeBackend();
    const storage = createStorageFromBackend(backend);

    const node1 = createTestNode();
    node1.setStorage(storage);

    const group = node1.createGroup();
    const map = group.createMap();
    map.set("key", "value");

    await new Promise<void>((resolve) => setTimeout(resolve, 100));

    expect(node1.syncManager.unsyncedTracker.has(map.id)).toBe(true);

    node1.gracefulShutdown();

    // Restart with same backend (fresh StorageApiAsync)
    const node2 = createTestNode();
    node2.setStorage(createStorageFromBackend(backend));

    const syncServer = createTestNode();
    connectToSyncServer(node2, syncServer);

    await node2.syncManager.waitForAllCoValuesSync();

    const getUnsyncedIDs = () =>
      new Promise<string[]>((resolve) =>
        node2.storage?.getUnsyncedCoValueIDs(resolve),
      );

    await waitFor(async () => (await getUnsyncedIDs()).length === 0);

    await node2.gracefulShutdown();
  });
});

test("should sync and load accounts from storage", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();
  const backend = createBfTreeBackend();

  const { node: node1, accountID } = await LocalNode.withNewlyCreatedAccount({
    crypto: Crypto,
    initialAgentSecret: agentSecret,
    storage: createStorageFromBackend(backend),
    creationProps: { name: "test" },
  });

  node1.gracefulShutdown();
  syncMessages.restore();
  syncMessages = trackMessages();

  const node2 = await LocalNode.withLoadedAccount({
    crypto: Crypto,
    accountSecret: agentSecret,
    accountID,
    peers: [],
    storage: createStorageFromBackend(backend),
    sessionID: Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
  });

  expect(node2.getCoValue(accountID).isAvailable()).toBeTruthy();
});
