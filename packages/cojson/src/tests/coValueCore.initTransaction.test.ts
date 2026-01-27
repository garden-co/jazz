import { beforeEach, describe, expect, test, vi } from "vitest";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import {
  createTwoConnectedNodes,
  importContentIntoNode,
  loadCoValueOrFail,
  setupTestNode,
  waitFor,
} from "./testUtils.js";

beforeEach(() => {
  setupTestNode({ isSyncServer: true });
});

describe("init transaction meta", () => {
  test("a transaction with init meta is parsed correctly", () => {
    const client = setupTestNode();
    const group = client.node.createGroup();
    const map = group.createMap();

    map.core.makeTransaction(
      [{ op: "set", key: "hello", value: "world" }],
      "trusting",
      {
        init: true,
      },
    );

    const transactions = map.core.getValidSortedTransactions();
    expect(transactions).toHaveLength(1);
    expect(transactions[0]?.meta).toEqual({ init: true });
  });

  test("first-init-wins: only the first init transaction is valid", () => {
    const client = setupTestNode();
    const group = client.node.createGroup();
    const map = group.createMap();

    // Make two init transactions with different timestamps
    // The first one (earlier madeAt) should win
    const earlierTime = Date.now();
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "first" }],
      "trusting",
      { init: true },
      earlierTime,
    );

    const laterTime = earlierTime + 100;
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "second" }],
      "trusting",
      { init: true },
      laterTime,
    );

    const validTransactions = map.core.getValidSortedTransactions();

    // Only the first init transaction should be valid
    expect(validTransactions).toHaveLength(1);
    expect(validTransactions[0]?.meta).toEqual({ init: true });

    // The first transaction (earlier madeAt) should be the valid one
    expect(validTransactions[0]?.madeAt).toBe(earlierTime);
  });

  test("first-init-wins: transactions without init meta are not affected", () => {
    const client = setupTestNode();
    const group = client.node.createGroup();
    const map = group.createMap();

    // Make an init transaction
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "init" }],
      "trusting",
      { init: true },
    );

    // Make a regular transaction (no init meta)
    map.core.makeTransaction(
      [{ op: "set", key: "hello", value: "world" }],
      "trusting",
    );

    const validTransactions = map.core.getValidSortedTransactions();

    // Both transactions should be valid
    expect(validTransactions).toHaveLength(2);
  });

  test("late-arriving winner triggers content rebuild", async () => {
    const client = setupTestNode({ connected: true });
    const clientSession2 = client.spawnNewSession();
    const group = client.node.createGroup();
    const map = group.createMap();
    const mapOnClientSession2 = await loadCoValueOrFail(
      clientSession2.node,
      map.id,
    );

    // Make an init transaction with a later timestamp
    const laterTime = Date.now() + 1000;
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "later" }],
      "trusting",
      { init: true },
      laterTime,
    );

    expect(map.get("version")).toBe("later");

    const rebuildSpy = vi.spyOn(map, "rebuildFromCore");

    // Now make an init transaction with an earlier timestamp (this should win)
    const earlierTime = laterTime - 500;
    mapOnClientSession2.core.makeTransaction(
      [{ op: "set", key: "version", value: "earlier" }],
      "trusting",
      { init: true },
      earlierTime,
    );

    await waitFor(() => {
      expect(map.core.knownState()).toEqual(
        mapOnClientSession2.core.knownState(),
      );
    });

    // The content should have been rebuilt
    expect(rebuildSpy).toHaveBeenCalled();

    expect(map.get("version")).toBe("earlier");
  });

  test("two init transactions coming together do not trigger content rebuild", async () => {
    const alice = setupTestNode({ connected: true });
    const aliceSession2 = alice.spawnNewSession();
    const bob = setupTestNode({ connected: false });
    const group = alice.node.createGroup();
    group.addMember("everyone", "writer");
    const map = group.createMap();

    importContentIntoNode(group.core, bob.node);
    importContentIntoNode(map.core, bob.node);

    const mapOnBob = bob.node.getCoValue(map.id);
    const rebuildSpy = vi.spyOn(mapOnBob, "scheduleContentRebuild");

    const mapOnAliceSession2 = await loadCoValueOrFail(
      aliceSession2.node,
      map.id,
    );

    // Make an init transaction with a later timestamp
    const laterTime = Date.now() + 1000;
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "later" }],
      "trusting",
      { init: true },
      laterTime,
    );

    expect(map.get("version")).toBe("later");

    // Now make an init transaction with an earlier timestamp (this should win)
    const earlierTime = laterTime - 500;
    mapOnAliceSession2.core.makeTransaction(
      [{ op: "set", key: "version", value: "earlier" }],
      "trusting",
      { init: true },
      earlierTime,
    );

    await waitFor(() => {
      expect(map.core.knownState()).toEqual(
        mapOnAliceSession2.core.knownState(),
      );
    });

    importContentIntoNode(map.core, bob.node);

    // The content should have been rebuilt
    expect(rebuildSpy).not.toHaveBeenCalled();

    expect(mapOnBob.getCurrentContent().toJSON()).toEqual({
      version: "earlier",
    });
    expect(map.get("version")).toBe("earlier");
  });

  test("content reflects the winning init transaction after rebuild", async () => {
    const client = setupTestNode({ connected: true });
    const clientSession2 = client.spawnNewSession();
    const group = client.node.createGroup();
    const map = group.createMap();
    const mapOnClientSession2 = await loadCoValueOrFail(
      clientSession2.node,
      map.id,
    );

    // Make an init transaction with a later timestamp
    const laterTime = Date.now() + 1000;
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "later" }],
      "trusting",
      { init: true },
      laterTime,
    );

    // Verify initial content
    expect(map.get("version")).toBe("later");

    // Now make an init transaction with an earlier timestamp (this should win)
    const earlierTime = laterTime - 500;
    mapOnClientSession2.core.makeTransaction(
      [{ op: "set", key: "version", value: "earlier" }],
      "trusting",
      { init: true },
      earlierTime,
    );

    await waitFor(() => {
      expect(map.core.knownState()).toEqual(
        mapOnClientSession2.core.knownState(),
      );
    });

    // The content should reflect the earlier (winning) init transaction
    expect(map.get("version")).toBe("earlier");
  });

  test("losing init transaction is marked as invalid (different sessions)", async () => {
    const client = setupTestNode({ connected: true });
    const clientSession2 = client.spawnNewSession();
    const group = client.node.createGroup();
    const map = group.createMap();

    const mapOnClientSession2 = await loadCoValueOrFail(
      clientSession2.node,
      map.id,
    );

    const earlierTime = Date.now();
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "first" }],
      "trusting",
      { init: true },
      earlierTime,
    );

    const laterTime = earlierTime + 100;
    mapOnClientSession2.core.makeTransaction(
      [{ op: "set", key: "version", value: "second" }],
      "trusting",
      { init: true },
      laterTime,
    );

    await waitFor(() => {
      expect(map.core.knownState()).toEqual(
        mapOnClientSession2.core.knownState(),
      );
    });

    // Check the raw verified transactions
    const allTransactions = map.core.verifiedTransactions;
    expect(allTransactions).toHaveLength(2);

    // The first transaction should be valid
    const firstTx = allTransactions.find((tx) => tx.madeAt === earlierTime);
    expect(firstTx?.isValid).toBe(true);

    // The second transaction should be invalid
    const secondTx = allTransactions.find((tx) => tx.madeAt === laterTime);
    expect(secondTx?.isValid).toBe(false);
    expect(secondTx?.validationErrorMessage).toBe(
      "Transaction is not the first init transaction",
    );
  });

  test("losing init transaction is marked as invalid (same session)", () => {
    const client = setupTestNode();
    const group = client.node.createGroup();
    const map = group.createMap();

    const earlierTime = Date.now();
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "first" }],
      "trusting",
      { init: true },
      earlierTime,
    );

    const laterTime = earlierTime + 100;
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "second" }],
      "trusting",
      { init: true },
      laterTime,
    );

    // Check the raw verified transactions
    const allTransactions = map.core.verifiedTransactions;
    expect(allTransactions).toHaveLength(2);

    // The first transaction should be valid
    const firstTx = allTransactions.find((tx) => tx.madeAt === earlierTime);
    expect(firstTx?.isValid).toBe(true);

    // The second transaction should be invalid
    const secondTx = allTransactions.find((tx) => tx.madeAt === laterTime);
    expect(secondTx?.isValid).toBe(false);
    expect(secondTx?.validationErrorMessage).toBe(
      "Init transaction must be the first transaction in its session",
    );
  });

  test("validity change on processed transaction dispatches rebuild", async () => {
    const client = setupTestNode({ connected: true });
    const clientSession2 = client.spawnNewSession();
    const group = client.node.createGroup();
    const map = group.createMap();

    const mapOnClientSession2 = await loadCoValueOrFail(
      clientSession2.node,
      map.id,
    );

    // Make an init transaction and process it
    const laterTime = Date.now() + 1000;
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "later" }],
      "trusting",
      { init: true },
      laterTime,
    );

    // Check the transaction is marked as processed
    const laterTx = map.core.verifiedTransactions.find(
      (tx) => tx.madeAt === laterTime,
    );
    expect(laterTx?.stage).toBe("processed");
    expect(laterTx?.isValid).toBe(true);

    // Add a new init transaction with an earlier timestamp
    const earlierTime = laterTime - 500;
    mapOnClientSession2.core.makeTransaction(
      [{ op: "set", key: "version", value: "earlier" }],
      "trusting",
      { init: true },
      earlierTime,
    );

    await waitFor(() => {
      expect(map.core.knownState()).toEqual(
        mapOnClientSession2.core.knownState(),
      );
    });

    // The later transaction should now be invalid
    expect(laterTx?.isValid).toBe(false);
  });

  test("synced init transactions resolve correctly across nodes", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap();

    // Make an init transaction on node1
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "node1" }],
      "trusting",
      { init: true },
    );

    await map.core.waitForSync();

    // Load the map on node2
    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    await waitFor(() => {
      expect(mapOnNode2.get("version")).toBe("node1");
    });

    // Both nodes should have the same valid transaction
    const node1ValidTxs = map.core.getValidSortedTransactions();
    const node2ValidTxs = mapOnNode2.core.getValidSortedTransactions();

    expect(node1ValidTxs).toHaveLength(1);
    expect(node2ValidTxs).toHaveLength(1);
    expect(node1ValidTxs[0]?.madeAt).toBe(node2ValidTxs[0]?.madeAt);
  });

  test("concurrent init transactions from different nodes resolve deterministically", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap();

    await map.core.waitForSync();

    // Load the map on node2
    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    // Make init transactions on both nodes with different timestamps
    const node1Time = Date.now();
    const node2Time = node1Time + 1000; // node2 is later

    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "node1" }],
      "trusting",
      { init: true },
      node1Time,
    );

    mapOnNode2.core.makeTransaction(
      [{ op: "set", key: "version", value: "node2" }],
      "trusting",
      { init: true },
      node2Time,
    );

    // Wait for sync
    await map.core.waitForSync();
    await mapOnNode2.core.waitForSync();

    // Wait for microtasks
    await new Promise<void>((resolve) => queueMicrotask(resolve));

    // Both nodes should converge to the same winner (node1 with earlier timestamp)
    await waitFor(() => {
      expect(map.get("version")).toBe("node1");
      expect(mapOnNode2.get("version")).toBe("node1");
    });
  });

  test("subscription is notified when init transaction changes the content", async () => {
    const client = setupTestNode();
    const group = client.node.createGroup();
    const map = group.createMap();

    // Make an init transaction with a later timestamp
    const laterTime = Date.now() + 1000;
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "later" }],
      "trusting",
      { init: true },
      laterTime,
    );

    // Subscribe to changes
    const subscriptionSpy = vi.fn();
    const unsubscribe = map.subscribe(subscriptionSpy);

    subscriptionSpy.mockClear();

    // Add a new init transaction with an earlier timestamp
    const earlierTime = laterTime - 500;
    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "earlier" }],
      "trusting",
      { init: true },
      earlierTime,
    );

    // Wait for notifications
    await waitFor(() => {
      expect(subscriptionSpy).toHaveBeenCalled();
    });

    unsubscribe();
  });

  test("getValidTransactions returns discarded init transactions when includeInvalidMetaTransactions is true", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap();

    const earlierTime = Date.now();
    const laterTime = earlierTime + 1000;

    map.core.makeTransaction(
      [{ op: "set", key: "version", value: "later" }],
      "trusting",
      { init: true },
      laterTime,
    );

    await map.core.waitForSync();

    const mapOnNode2 = await loadCoValueOrFail(node2.node, map.id);

    await waitFor(() => {
      expect(mapOnNode2.get("version")).toBe("later");
    });

    mapOnNode2.core.makeTransaction(
      [{ op: "set", key: "version", value: "earlier" }],
      "trusting",
      { init: true },
      earlierTime,
    );

    await waitFor(() => {
      expect(map.core.knownState()).toEqual(mapOnNode2.core.knownState());
    });

    // Without flag: only valid transactions
    const validOnly = map.core.getValidSortedTransactions();
    expect(validOnly).toHaveLength(1);
    expect(validOnly[0]?.madeAt).toBe(earlierTime);

    // With flag: includes invalid init transactions
    const withInvalid = map.core.getValidSortedTransactions({
      includeInvalidMetaTransactions: true,
      ignorePrivateTransactions: false,
    });
    expect(withInvalid).toHaveLength(2);
    expect(withInvalid.filter((tx) => tx.isValid)).toHaveLength(1);
    expect(withInvalid.filter((tx) => !tx.isValid)).toHaveLength(1);
  });

  test("getValidTransactions does not return permission-invalid transactions", async () => {
    const { node1, node2 } = await createTwoConnectedNodes("server", "server");

    const group = node1.node.createGroup();
    const map = group.createMap();

    group.addMember(node2.node.getCurrentAgent(), "reader");

    map.set("key", "admin-value", "trusting");

    await map.core.waitForSync();

    const mapOnReader = await loadCoValueOrFail(node2.node, map.id);

    await waitFor(() => {
      expect(mapOnReader.get("key")).toBe("admin-value");
    });

    mapOnReader.set("key", "reader-value", "trusting");

    // Permission-invalid transactions are never included
    const allTx = mapOnReader.core.getValidSortedTransactions({
      includeInvalidMetaTransactions: true,
      ignorePrivateTransactions: false,
    });

    expect(allTx).toHaveLength(1);
    expect(mapOnReader.get("key")).toBe("admin-value");
  });
});
