import { LocalNode, StorageApiAsync } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { afterEach, beforeEach, expect, test, vi } from "vitest";
import { getIndexedDBStorage } from "../index.js";
import { toSimplifiedMessages } from "./messagesTestUtils.js";
import { trackMessages, waitFor } from "./testUtils.js";

const Crypto = await WasmCrypto.create();
let syncMessages: ReturnType<typeof trackMessages>;

beforeEach(() => {
  syncMessages = trackMessages();
});

afterEach(() => {
  syncMessages.restore();
});

test("should sync and load data from storage", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );
  node1.setStorage(await getIndexedDBStorage());

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
      "client -> CONTENT Group header: true new: After: 0 New: 3",
      "client -> CONTENT Map header: true new: After: 0 New: 1",
    ]
  `);

  node1.gracefulShutdown();
  syncMessages.clear();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node2.setStorage(await getIndexedDBStorage());

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
      "storage -> CONTENT Group header: true new: After: 0 New: 3",
      "storage -> CONTENT Map header: true new: After: 0 New: 1",
    ]
  `);
});

test("should send an empty content message if there is no content", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node1.setStorage(await getIndexedDBStorage());

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
      "client -> CONTENT Group header: true new: After: 0 New: 3",
      "client -> CONTENT Map header: true new: ",
    ]
  `);

  syncMessages.clear();
  node1.gracefulShutdown();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node2.setStorage(await getIndexedDBStorage());

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
      "storage -> CONTENT Group header: true new: After: 0 New: 3",
      "storage -> CONTENT Map header: true new: ",
    ]
  `);
});

test("should load dependencies correctly (group inheritance)", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node1.setStorage(await getIndexedDBStorage());
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
      "client -> CONTENT Group header: true new: After: 0 New: 3",
      "client -> CONTENT ParentGroup header: true new: After: 0 New: 4",
      "client -> CONTENT Group header: false new: After: 3 New: 2",
      "client -> CONTENT Map header: true new: After: 0 New: 1",
    ]
  `);

  syncMessages.clear();
  node1.gracefulShutdown();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node2.setStorage(await getIndexedDBStorage());

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
      "storage -> CONTENT Group header: true new: After: 0 New: 5",
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

  node1.setStorage(await getIndexedDBStorage());

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

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node2.setStorage(await getIndexedDBStorage());

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
      "storage -> CONTENT Group header: true new: After: 0 New: 5",
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

  const storage = await getIndexedDBStorage();
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
      "client -> CONTENT Group header: true new: After: 0 New: 3",
      "client -> CONTENT Map header: true new: After: 0 New: 1",
      "client -> CONTENT Map header: false new: After: 3 New: 1",
      "storage -> KNOWN CORRECTION Map sessions: header/4",
      "client -> CONTENT Map header: false new: After: 1 New: 3",
    ]
  `);

  syncMessages.clear();
  node1.gracefulShutdown();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node2.setStorage(await getIndexedDBStorage());

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
      "storage -> CONTENT Group header: true new: After: 0 New: 3",
      "storage -> CONTENT Map header: true new: After: 0 New: 4",
    ]
  `);
});

test("should sync multiple sessions in a single content message", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node1.setStorage(await getIndexedDBStorage());

  const group = node1.createGroup();

  const map = group.createMap();

  map.set("hello", "world");

  await map.core.waitForSync();

  node1.gracefulShutdown();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node2.setStorage(await getIndexedDBStorage());

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

  syncMessages.clear();

  node3.setStorage(await getIndexedDBStorage());

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
      "storage -> CONTENT Group header: true new: After: 0 New: 3",
      "storage -> CONTENT Map header: true new: After: 0 New: 1 | After: 0 New: 1",
    ]
  `);
});

test("large coValue upload streaming", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const node1 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  node1.setStorage(await getIndexedDBStorage());

  const group = node1.createGroup();
  const largeMap = group.createMap();

  // Generate a large amount of data (about 100MB)
  const dataSize = 1 * 1024 * 200;
  const chunkSize = 1024; // 1KB chunks
  const chunks = dataSize / chunkSize;

  const value = "a".repeat(chunkSize);

  for (let i = 0; i < chunks; i++) {
    const key = `key${i}`;
    largeMap.set(key, value, "trusting");
  }

  // TODO: Wait for storage to be updated
  await largeMap.core.waitForSync();

  const knownState = largeMap.core.knownState();

  node1.gracefulShutdown();

  const node2 = new LocalNode(
    agentSecret,
    Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
    Crypto,
  );

  syncMessages.clear();

  node2.setStorage(await getIndexedDBStorage());

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
      "storage -> CONTENT Group header: true new: After: 0 New: 3",
      "storage -> CONTENT Map header: true new: After: 0 New: 97",
      "storage -> CONTENT Map header: true new: After: 97 New: 97",
      "storage -> CONTENT Map header: true new: After: 194 New: 6",
    ]
  `);
});

test("should sync and load accounts from storage", async () => {
  const agentSecret = Crypto.newRandomAgentSecret();

  const { node: node1, accountID } = await LocalNode.withNewlyCreatedAccount({
    crypto: Crypto,
    initialAgentSecret: agentSecret,
    storage: await getIndexedDBStorage(),
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
      "client -> CONTENT ProfileGroup header: true new: After: 0 New: 5",
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
    peersToLoadFrom: [],
    storage: await getIndexedDBStorage(),
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
      "storage -> CONTENT ProfileGroup header: true new: After: 0 New: 5",
      "storage -> CONTENT Profile header: true new: After: 0 New: 1",
    ]
  `);

  expect(node2.getCoValue(accountID).isAvailable()).toBeTruthy();
});
