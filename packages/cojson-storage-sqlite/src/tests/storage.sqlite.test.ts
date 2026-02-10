import { randomUUID } from "node:crypto";
import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LocalNode, StorageApiSync, cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { expect, onTestFinished, test, vi } from "vitest";
import { getBetterSqliteStorage } from "../index.js";
import { toSimplifiedMessages } from "./messagesTestUtils.js";
import { trackMessages, waitFor } from "./testUtils.js";

const Crypto = await WasmCrypto.create();

function createSQLiteStorage(defaultDbPath?: string) {
  const dbPath = defaultDbPath ?? join(tmpdir(), `test-${randomUUID()}.db`);

  if (!defaultDbPath) {
    onTestFinished(() => {
      unlinkSync(dbPath);
    });
  }

  return {
    storage: getBetterSqliteStorage(dbPath),
    dbPath,
  };
}

test("should sync and load data from storage", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node1Sync = trackMessages();

  const { storage, dbPath } = createSQLiteStorage();

  node1.setStorage(storage);

  const group = node1.createGroup();

  const map = group.createMap();

  map.set("hello", "world");

  await new Promise((resolve) => setTimeout(resolve, 200));

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      node1Sync.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> CONTENT Group header: true new: After: 0 New: 4",
      "client -> CONTENT Map header: true new: After: 0 New: 1",
    ]
  `);

  node1Sync.restore();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node2Sync = trackMessages();

  node2.setStorage(createSQLiteStorage(dbPath).storage);

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
      node2Sync.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Map sessions: empty",
      "storage -> CONTENT Group header: true new: After: 0 New: 4",
      "storage -> CONTENT Map header: true new: After: 0 New: 1",
    ]
  `);

  node2Sync.restore();
});

test("should send an empty content message if there is no content", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node1Sync = trackMessages();

  const { storage, dbPath } = createSQLiteStorage();

  node1.setStorage(storage);

  const group = node1.createGroup();

  const map = group.createMap();

  await new Promise((resolve) => setTimeout(resolve, 200));

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      node1Sync.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> CONTENT Group header: true new: After: 0 New: 4",
      "client -> CONTENT Map header: true new: ",
    ]
  `);

  node1Sync.restore();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node2Sync = trackMessages();

  node2.setStorage(createSQLiteStorage(dbPath).storage);

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
      node2Sync.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Map sessions: empty",
      "storage -> CONTENT Group header: true new: After: 0 New: 4",
      "storage -> CONTENT Map header: true new: ",
    ]
  `);

  node2Sync.restore();
});

test("should load dependencies correctly (group inheritance)", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node1Sync = trackMessages();

  const { storage, dbPath } = createSQLiteStorage();

  node1.setStorage(storage);

  const group = node1.createGroup();
  const parentGroup = node1.createGroup();

  group.extend(parentGroup);

  const map = group.createMap();

  map.set("hello", "world");

  await new Promise((resolve) => setTimeout(resolve, 200));

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
        ParentGroup: parentGroup.core,
      },
      node1Sync.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> CONTENT Group header: true new: After: 0 New: 4",
      "client -> CONTENT ParentGroup header: true new: After: 0 New: 4",
      "client -> CONTENT Group header: false new: After: 4 New: 2",
      "client -> CONTENT Map header: true new: After: 0 New: 1",
    ]
  `);

  node1Sync.restore();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node2Sync = trackMessages();

  node2.setStorage(createSQLiteStorage(dbPath).storage);

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
      node2Sync.messages,
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
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node1Sync = trackMessages();

  const { storage, dbPath } = createSQLiteStorage();

  node1.setStorage(storage);

  const group = node1.createGroup();
  const parentGroup = node1.createGroup();

  group.extend(parentGroup);

  const mapFromParent = parentGroup.createMap();
  const map = group.createMap();

  map.set("hello", "world");
  mapFromParent.set("hello", "world");

  await new Promise((resolve) => setTimeout(resolve, 200));

  node1Sync.restore();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node2Sync = trackMessages();

  node2.setStorage(createSQLiteStorage(dbPath).storage);

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
      node2Sync.messages,
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
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node1Sync = trackMessages();

  const { storage, dbPath } = createSQLiteStorage();

  node1.setStorage(storage);

  const group = node1.createGroup();

  const map = group.createMap();

  map.set("0", 0);

  await new Promise((resolve) => setTimeout(resolve, 200));

  const mock = vi
    .spyOn(StorageApiSync.prototype, "store")
    .mockImplementation(() => false);

  map.set("1", 1);
  map.set("2", 2);

  await new Promise((resolve) => setTimeout(resolve, 200));

  mock.mockReset();

  map.set("3", 3);

  await new Promise((resolve) => setTimeout(resolve, 200));

  expect(
    toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      node1Sync.messages,
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

  node1Sync.restore();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node2Sync = trackMessages();

  node2.setStorage(createSQLiteStorage(dbPath).storage);

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
      node2Sync.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Map sessions: empty",
      "storage -> CONTENT Group header: true new: After: 0 New: 4",
      "storage -> CONTENT Map header: true new: After: 0 New: 4",
    ]
  `);
});

test("should recover missing dependencies from storage", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const account = LocalNode.internalCreateAccount({
    crypto: Crypto,
  });
  const node1 = account.core.node;

  const serverNode = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const [serverPeer, clientPeer] = cojsonInternals.connectedPeers(
    node1.agentSecret,
    serverNode.agentSecret,
    {
      peer1role: "server",
      peer2role: "client",
    },
  );

  node1.syncManager.addPeer(serverPeer);
  serverNode.syncManager.addPeer(clientPeer);

  const store = StorageApiSync.prototype.store;

  const mock = vi
    .spyOn(StorageApiSync.prototype, "store")
    .mockImplementation(function (
      this: StorageApiSync,
      data,
      correctionCallback,
    ) {
      if ([group.core.id, account.core.id as string].includes(data.id)) {
        return false;
      }

      return store.call(this, data, correctionCallback);
    });

  const { storage, dbPath } = createSQLiteStorage();

  node1.setStorage(storage);

  const group = node1.createGroup();
  group.addMember("everyone", "writer");

  const map = group.createMap();

  map.set("0", 0);

  mock.mockReset();

  await new Promise((resolve) => setTimeout(resolve, 200));

  const node2 = new LocalNode(
    Crypto.newRandomAgentSecret(),
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const [serverPeer2, clientPeer2] = cojsonInternals.connectedPeers(
    node1.agentSecret,
    serverNode.agentSecret,
    {
      peer1role: "server",
      peer2role: "client",
    },
  );

  node2.syncManager.addPeer(serverPeer2);
  serverNode.syncManager.addPeer(clientPeer2);

  node2.setStorage(createSQLiteStorage(dbPath).storage);

  const map2 = await node2.load(map.id);

  if (map2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map2.toJSON()).toEqual({
    "0": 0,
  });
});

test("should sync multiple sessions in a single content message", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const { storage, dbPath } = createSQLiteStorage();

  node1.setStorage(storage);

  const group = node1.createGroup();

  const map = group.createMap();

  map.set("hello", "world");

  await new Promise((resolve) => setTimeout(resolve, 200));

  node1.gracefulShutdown();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node2.setStorage(createSQLiteStorage(dbPath).storage);

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map2.get("hello")).toBe("world");

  map2.set("hello", "world2");

  await map2.core.waitForSync();

  node2.gracefulShutdown();

  const node3 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node3Sync = trackMessages();

  node3.setStorage(createSQLiteStorage(dbPath).storage);

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
      node3Sync.messages,
    ),
  ).toMatchInlineSnapshot(`
    [
      "client -> LOAD Map sessions: empty",
      "storage -> CONTENT Group header: true new: After: 0 New: 4",
      "storage -> CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
    ]
  `);

  node3Sync.restore();
});

test("large coValue upload streaming", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const { storage, dbPath } = createSQLiteStorage();

  node1.setStorage(storage);

  const group = node1.createGroup();
  const largeMap = group.createMap();

  const dataSize = 1 * 1024 * 200;
  const chunkSize = 1024; // 1KB chunks
  const chunks = dataSize / chunkSize;

  const value = "a".repeat(chunkSize);

  for (let i = 0; i < chunks; i++) {
    const key = `key${i}`;
    largeMap.set(key, value, "trusting");
  }

  await largeMap.core.waitForSync();

  node1.gracefulShutdown();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node2Sync = trackMessages();

  node2.setStorage(createSQLiteStorage(dbPath).storage);

  const largeMapOnNode2 = await node2.load(largeMap.id);

  if (largeMapOnNode2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  await waitFor(() => {
    expect(largeMapOnNode2.core.knownState()).toEqual(
      largeMap.core.knownState(),
    );

    return true;
  });

  expect(
    toSimplifiedMessages(
      {
        Map: largeMap.core,
        Group: group.core,
      },
      node2Sync.messages,
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
