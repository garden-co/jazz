import { randomUUID } from "node:crypto";
import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LocalNode, cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { expect, onTestFinished, test, vi } from "vitest";
import { toSimplifiedMessages } from "./messagesTestUtils.js";
import { trackMessages, waitFor } from "./testUtils.js";

const Crypto = await WasmCrypto.create();

import Database, { type Database as DatabaseT } from "libsql";
import { StorageManagerAsync } from "../managerAsync.js";
import { SQLiteNodeBaseAsync } from "../sqliteAsync/node.js";
import type { SQLiteDatabaseDriverAsync } from "../sqliteAsync/types.js";

class LibSQLSqliteDriver implements SQLiteDatabaseDriverAsync {
  private readonly db: DatabaseT;

  constructor(filename: string) {
    this.db = new Database(filename, {});
  }

  async initialize() {
    await this.db.pragma("journal_mode = WAL");
  }

  async run(sql: string, params: unknown[]) {
    this.db.prepare(sql).run(params);
  }

  async query<T>(sql: string, params: unknown[]): Promise<T[]> {
    return this.db.prepare(sql).all(params) as T[];
  }

  async get<T>(sql: string, params: unknown[]): Promise<T | undefined> {
    return this.db.prepare(sql).get(params) as T | undefined;
  }

  async transaction(callback: () => unknown) {
    await this.run("BEGIN TRANSACTION", []);

    try {
      await callback();
      await this.run("COMMIT", []);
    } catch (error) {
      await this.run("ROLLBACK", []);
    }
  }

  async closeDb() {
    this.db.close();
  }
}

async function createSQLiteStorage(defaultDbPath?: string) {
  const dbPath = defaultDbPath ?? join(tmpdir(), `test-${randomUUID()}.db`);

  if (!defaultDbPath) {
    onTestFinished(() => {
      unlinkSync(dbPath);
    });
  }

  const db = new LibSQLSqliteDriver(dbPath);

  return {
    peer: await SQLiteNodeBaseAsync.create({
      db,
    }),
    dbPath,
    db,
  };
}

test("Should be able to initialize and load from empty DB", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node.syncManager.addPeer((await createSQLiteStorage()).peer);

  await new Promise((resolve) => setTimeout(resolve, 200));

  expect(node.syncManager.peers.storage).toBeDefined();
});

test("should sync and load data from storage", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node1Sync = trackMessages(node1);

  const { peer, dbPath } = await createSQLiteStorage();

  node1.syncManager.addPeer(peer);

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
      "client -> CONTENT Group header: true new: After: 0 New: 3",
      "storage -> KNOWN Group sessions: header/3",
      "client -> CONTENT Map header: true new: After: 0 New: 1",
      "storage -> KNOWN Map sessions: header/1",
    ]
  `);

  node1Sync.restore();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node2Sync = trackMessages(node2);

  const { peer: peer2 } = await createSQLiteStorage(dbPath);

  node2.syncManager.addPeer(peer2);

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
      "storage -> CONTENT Group header: true new: After: 0 New: 3",
      "client -> KNOWN Group sessions: header/3",
      "storage -> CONTENT Map header: true new: After: 0 New: 1",
      "client -> KNOWN Map sessions: header/1",
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

  const node1Sync = trackMessages(node1);

  const { peer, dbPath } = await createSQLiteStorage();

  node1.syncManager.addPeer(peer);

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
      "client -> CONTENT Group header: true new: After: 0 New: 3",
      "storage -> KNOWN Group sessions: header/3",
      "client -> CONTENT Map header: true new: ",
      "storage -> KNOWN Map sessions: header/0",
    ]
  `);

  node1Sync.restore();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node2Sync = trackMessages(node2);

  const { peer: peer2 } = await createSQLiteStorage(dbPath);

  node2.syncManager.addPeer(peer2);

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
      "storage -> CONTENT Group header: true new: After: 0 New: 3",
      "client -> KNOWN Group sessions: header/3",
      "storage -> CONTENT Map header: true new: ",
      "client -> KNOWN Map sessions: header/0",
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

  const node1Sync = trackMessages(node1);

  const { peer, dbPath } = await createSQLiteStorage();

  node1.syncManager.addPeer(peer);

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
      "client -> CONTENT ParentGroup header: true new: After: 0 New: 4",
      "storage -> KNOWN ParentGroup sessions: header/4",
      "client -> CONTENT Group header: true new: After: 0 New: 5",
      "storage -> KNOWN Group sessions: header/5",
      "client -> CONTENT Map header: true new: After: 0 New: 1",
      "storage -> KNOWN Map sessions: header/1",
    ]
  `);

  node1Sync.restore();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node2Sync = trackMessages(node2);

  const { peer: peer2 } = await createSQLiteStorage(dbPath);

  node2.syncManager.addPeer(peer2);

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
      "client -> KNOWN ParentGroup sessions: header/4",
      "storage -> CONTENT Group header: true new: After: 0 New: 5",
      "client -> KNOWN Group sessions: header/5",
      "storage -> CONTENT Map header: true new: After: 0 New: 1",
      "client -> KNOWN Map sessions: header/1",
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

  const node1Sync = trackMessages(node1);

  const { peer, dbPath } = await createSQLiteStorage();

  node1.syncManager.addPeer(peer);

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

  const node2Sync = trackMessages(node2);

  const { peer: peer2 } = await createSQLiteStorage(dbPath);

  node2.syncManager.addPeer(peer2);

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
      "client -> KNOWN ParentGroup sessions: header/4",
      "storage -> CONTENT Group header: true new: After: 0 New: 5",
      "client -> KNOWN Group sessions: header/5",
      "storage -> CONTENT Map header: true new: After: 0 New: 1",
      "client -> KNOWN Map sessions: header/1",
      "client -> LOAD MapFromParent sessions: empty",
      "storage -> CONTENT MapFromParent header: true new: After: 0 New: 1",
      "client -> KNOWN MapFromParent sessions: header/1",
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

  const node1Sync = trackMessages(node1);

  const { peer, dbPath } = await createSQLiteStorage();

  node1.syncManager.addPeer(peer);

  const group = node1.createGroup();

  const map = group.createMap();

  map.set("0", 0);

  await new Promise((resolve) => setTimeout(resolve, 200));

  const mock = vi
    .spyOn(StorageManagerAsync.prototype, "handleSyncMessage")
    .mockImplementation(() => Promise.resolve());

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
      "client -> CONTENT Group header: true new: After: 0 New: 3",
      "storage -> KNOWN Group sessions: header/3",
      "client -> CONTENT Map header: true new: After: 0 New: 1",
      "storage -> KNOWN Map sessions: header/1",
      "client -> CONTENT Map header: false new: After: 3 New: 1",
      "storage -> KNOWN CORRECTION Map sessions: header/1",
      "client -> CONTENT Map header: false new: After: 1 New: 3",
      "storage -> KNOWN Map sessions: header/4",
    ]
  `);

  node1Sync.restore();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const node2Sync = trackMessages(node2);

  const { peer: peer2 } = await createSQLiteStorage(dbPath);

  node2.syncManager.addPeer(peer2);

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
      "storage -> CONTENT Group header: true new: After: 0 New: 3",
      "client -> KNOWN Group sessions: header/3",
      "storage -> CONTENT Map header: true new: After: 0 New: 4",
      "client -> KNOWN Map sessions: header/4",
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

  const handleSyncMessage = StorageManagerAsync.prototype.handleSyncMessage;

  const mock = vi
    .spyOn(StorageManagerAsync.prototype, "handleSyncMessage")
    .mockImplementation(function (this: StorageManagerAsync, msg) {
      if (
        msg.action === "content" &&
        [group.core.id, account.core.id].includes(msg.id)
      ) {
        return Promise.resolve();
      }

      return handleSyncMessage.call(this, msg);
    });

  const { peer, dbPath } = await createSQLiteStorage();

  node1.syncManager.addPeer(peer);

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

  const { peer: peer2 } = await createSQLiteStorage(dbPath);

  node2.syncManager.addPeer(peer2);

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

  const { peer, dbPath } = await createSQLiteStorage();

  node1.syncManager.addPeer(peer);

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

  node2.syncManager.addPeer((await createSQLiteStorage(dbPath)).peer);

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

  const node3Sync = trackMessages(node3);

  node3.syncManager.addPeer((await createSQLiteStorage(dbPath)).peer);

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
      "storage -> CONTENT Group header: true new: After: 0 New: 3",
      "client -> KNOWN Group sessions: header/3",
      "storage -> CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
      "client -> KNOWN Map sessions: header/2",
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

  const { peer, dbPath } = await createSQLiteStorage();

  node1.syncManager.addPeer(peer);

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

  const node2Sync = trackMessages(node2);

  const { peer: peer2 } = await createSQLiteStorage(dbPath);

  node2.syncManager.addPeer(peer2);

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
      "storage -> KNOWN Map sessions: header/200",
      "storage -> CONTENT Group header: true new: After: 0 New: 3",
      "client -> KNOWN Group sessions: header/3",
      "storage -> CONTENT Map header: true new: After: 0 New: 97",
      "client -> KNOWN Map sessions: header/97",
      "storage -> CONTENT Map header: true new: After: 97 New: 97",
      "client -> KNOWN Map sessions: header/194",
      "storage -> CONTENT Map header: true new: After: 194 New: 6",
      "client -> KNOWN Map sessions: header/200",
    ]
  `);
});

test("should close the db when the node is closed", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  const { peer, db } = await createSQLiteStorage();

  const spy = vi.spyOn(db, "closeDb");

  node1.syncManager.addPeer(peer);

  await new Promise((resolve) => setTimeout(resolve, 10));

  expect(spy).not.toHaveBeenCalled();

  node1.gracefulShutdown();

  await new Promise((resolve) => setTimeout(resolve, 10));

  expect(spy).toHaveBeenCalled();
});
