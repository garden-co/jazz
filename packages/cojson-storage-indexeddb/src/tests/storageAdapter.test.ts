import {
  CoValueCore,
  ControlledAgent,
  LocalNode,
  MAX_RECOMMENDED_TX_SIZE,
  type RawCoID,
  type RawCoMap,
  type StorageAdapter,
} from "cojson";
import { cojsonInternals } from "cojson";
import type { CojsonInternalTypes } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { assert, describe, expect, test } from "vitest";
import { internal_setDatabaseName } from "../idbNode.js";
import { loadIDBStorageAdapter } from "../storageAdapter";

async function setup() {
  const crypto = await WasmCrypto.create();
  const agentSecret = crypto.newRandomAgentSecret();
  const agentID = crypto.getAgentID(agentSecret);

  // Use a unique database name for each test
  const dbName = `test-${Math.random().toString(36).slice(2)}`;
  internal_setDatabaseName(dbName);

  const storageAdapter = await loadIDBStorageAdapter();
  const node = new LocalNode(
    new ControlledAgent(agentSecret, crypto),
    crypto.newRandomSessionID(agentID),
    crypto,
  );

  const storageDriver = new TestStorageDriver(
    storageAdapter,
    new LocalNode(
      new ControlledAgent(agentSecret, crypto),
      crypto.newRandomSessionID(agentID),
      crypto,
    ),
  );

  function createTestNode() {
    const newNode = new LocalNode(
      new ControlledAgent(agentSecret, crypto),
      crypto.newRandomSessionID(agentID),
      crypto,
    );

    const [node1ToNode2Peer, node2ToNode1Peer] = cojsonInternals.connectedPeers(
      "node1ToNode2",
      "node2ToNode1",
      {
        peer1role: "server",
        peer2role: "server",
      },
    );

    node.syncManager.addPeer(node2ToNode1Peer);
    newNode.syncManager.addPeer(node1ToNode2Peer);

    return newNode;
  }

  return {
    storageAdapter,
    node,
    storageDriver,
    createTestNode,
    dbName,
  };
}

describe("IDBStorageAdapter", () => {
  test("should return null when getting non-existent CoValue", async () => {
    const { storageAdapter } = await setup();
    const result = await storageAdapter.get("non_existent_id" as RawCoID);
    expect(result).toBeNull();
  });

  test("should write and retrieve header", async () => {
    const { node, storageAdapter } = await setup();
    const coValue = node.createGroup().core;

    await storageAdapter.writeHeader(coValue.id, coValue.header);

    const result = await storageAdapter.get(coValue.id);
    expect(result).not.toBeNull();
    expect(result?.header).toEqual(coValue.header);
    expect(result?.sessions.size).toBe(0);
  });

  test("should append and retrieve session transactions", async () => {
    const { node, storageDriver } = await setup();
    const group = node.createGroup();
    const map = group.createMap({ name: "test" });

    await storageDriver.set(group.core);
    await storageDriver.set(map.core);

    await storageDriver.get(group.core.id);
    const result = await storageDriver.get(map.core.id);
    expect(result).not.toBeNull();

    const mapFromStorage = result?.getCurrentContent() as RawCoMap;
    expect(mapFromStorage).not.toBeNull();
    expect(mapFromStorage?.get("name")).toBe("test");
  });

  test("should handle multiple transactions for same CoValue", async () => {
    const { node, storageDriver } = await setup();
    const group = node.createGroup();
    await storageDriver.set(group.core);

    const map = group.createMap({ count: 1 });
    await storageDriver.set(map.core);

    map.set("count", 2, "trusting");
    await storageDriver.set(map.core);

    map.set("count", 3, "trusting");
    await storageDriver.set(map.core);

    await storageDriver.get(group.core.id);
    const result = await storageDriver.get(map.core.id);
    expect(result).not.toBeNull();

    const mapFromStorage = result?.getCurrentContent() as RawCoMap;
    expect(mapFromStorage).not.toBeNull();
    expect(mapFromStorage?.get("count")).toBe(3);
  });

  test("should handle big updates that require multiple signatures", async () => {
    const { node, storageDriver } = await setup();
    const group = node.createGroup();
    await storageDriver.set(group.core);

    const map = group.createMap({ value: "a".repeat(MAX_RECOMMENDED_TX_SIZE) });

    await storageDriver.set(map.core);

    map.set("value", "b".repeat(MAX_RECOMMENDED_TX_SIZE));

    await storageDriver.set(map.core);

    map.set("value", "c");

    await storageDriver.set(map.core);

    await storageDriver.get(group.core.id); // Load the dependency first
    const result = await storageDriver.get(map.core.id);
    expect(result).not.toBeNull();

    const mapFromStrorage = result?.getCurrentContent() as RawCoMap;
    expect(mapFromStrorage).not.toBeNull();
    expect(mapFromStrorage?.get("value")).toBe("c");
  });

  test("should handle multiple sessions for same CoValue", async () => {
    const { node, storageDriver, createTestNode } = await setup();
    const node2 = createTestNode();
    const group = node.createGroup();
    await storageDriver.set(group.core);

    const map = group.createMap({ count: 1 });
    await storageDriver.set(map.core);

    const mapOnNode2 = await node2.load(map.id);
    assert(mapOnNode2 !== "unavailable", "Map should be loaded");

    mapOnNode2.set("count", 2);
    mapOnNode2.set("count", 3);

    await storageDriver.set(mapOnNode2.core);

    await storageDriver.get(group.core.id);
    const result = await storageDriver.get(map.core.id);
    expect(result).not.toBeNull();

    const mapFromStorage = result?.getCurrentContent() as RawCoMap;
    expect(mapFromStorage).not.toBeNull();
    expect(mapFromStorage?.get("count")).toBe(3);
  });

  test("should work with LocalNode", async () => {
    const adapter = await loadIDBStorageAdapter();

    const { node, accountID, sessionID } =
      await LocalNode.withNewlyCreatedAccount({
        creationProps: { name: "test" },
        crypto: await WasmCrypto.create(),
        storageAdapter: adapter,
      });

    const group = node.createGroup();
    const map = group.createMap({ count: 1 });
    map.set("count", 2);
    map.set("count", 3);

    await new Promise((resolve) => setTimeout(resolve, 1000));

    const node2 = await LocalNode.withLoadedAccount({
      accountID: accountID,
      accountSecret: node.account.agentSecret,
      sessionID: sessionID,
      crypto: await WasmCrypto.create(),
      storageAdapter: adapter,
      peersToLoadFrom: [],
    });

    const mapOnNode2 = await node2.load(map.id);
    assert(mapOnNode2 !== "unavailable", "Map should be loaded");

    expect(mapOnNode2?.get("count")).toBe(3);
  });
});

class TestStorageDriver {
  private storageAdapter: StorageAdapter;
  private storedStates: Map<RawCoID, CojsonInternalTypes.CoValueKnownState> =
    new Map();
  private node: LocalNode;

  constructor(storageAdapter: StorageAdapter, node: LocalNode) {
    this.storageAdapter = storageAdapter;
    this.node = node;
  }

  async get(id: RawCoID) {
    const storedCoValue = await this.storageAdapter.get(id);

    if (!storedCoValue) {
      return null;
    }

    const core = new CoValueCore(storedCoValue.header, this.node);
    this.node.coValuesStore.setAsAvailable(core.id, core);

    for (const [sessionID, sessionLog] of storedCoValue.sessions) {
      let start = 0;
      for (const [signatureAt, signature] of Object.entries(
        sessionLog.signatureAfter,
      )) {
        if (!signature) {
          throw new Error(
            `Expected signature at ${signatureAt} for session ${sessionID}`,
          );
        }

        const position = Number.parseInt(signatureAt) + 1;

        const result = core.tryAddTransactions(
          sessionID,
          sessionLog.transactions.slice(start, position),
          undefined,
          signature,
        );

        if (result.isErr()) {
          console.error(result.error);
          throw result.error;
        }

        start = position;
      }

      if (start < sessionLog.transactions.length) {
        const result = core.tryAddTransactions(
          sessionID,
          sessionLog.transactions.slice(start),
          undefined,
          sessionLog.lastSignature,
        );

        if (result.isErr()) {
          console.error(result.error);
          throw result.error;
        }
      }
    }

    this.storedStates.set(id, core.knownState());

    return core;
  }

  async set(core: CoValueCore) {
    const currentState = this.storedStates.get(core.id);

    if (!currentState) {
      await this.storageAdapter.writeHeader(core.id, core.header);
    }

    const newContentPieces = core.newContentSince(currentState);

    if (!newContentPieces) {
      return;
    }

    const knownState = core.knownState();

    for (const piece of newContentPieces) {
      const entries = Object.entries(piece.new) as [
        keyof typeof piece.new,
        CojsonInternalTypes.SessionNewContent,
      ][];

      for (const [sessionID, sessionNewContent] of entries) {
        await this.storageAdapter.appendToSession(
          core.id,
          sessionID,
          sessionNewContent.after,
          sessionNewContent.newTransactions,
          sessionNewContent.lastSignature,
        );
      }
    }

    this.storedStates.set(core.id, knownState);
  }
}
