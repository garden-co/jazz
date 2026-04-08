import { beforeEach, describe, expect, test } from "vitest";

import type { RawCoMap } from "../exports.js";
import {
  SyncMessagesLog,
  TEST_NODE_CONFIG,
  loadCoValueOrFail,
  setupTestNode,
} from "./testUtils.js";
import {
  setupRecoveryActors,
  createSharedTaskMap,
  crashAfterServerAckBeforeLocalPersist,
  expectTaskFields,
  waitForRecovery,
} from "./recoveryTestHelpers.js";

TEST_NODE_CONFIG.withAsyncPeers = true;

function blockStorageWrites(storage: { store: (...args: any[]) => any }) {
  const original = storage.store;
  storage.store = () => {};
  return {
    unblock: () => {
      storage.store = original;
    },
  };
}

function waitForCondition(
  callback: () => boolean | void,
  { retries = 50, interval = 100 } = {},
) {
  return new Promise<void>((resolve, reject) => {
    let count = 0;
    const check = () => {
      try {
        const result = callback();
        if (result !== false) {
          resolve();
          return;
        }
      } catch {
        // retry
      }
      if (++count > retries) {
        reject(new Error(`Condition not met after ${retries} retries`));
        return;
      }
      setTimeout(check, interval);
    };
    check();
  });
}

describe("signature mismatch recovery", () => {
  let jazzCloud: ReturnType<typeof setupTestNode>;

  beforeEach(() => {
    SyncMessagesLog.clear();
    jazzCloud = setupTestNode({ isSyncServer: true });
  });

  test("server sends SignatureMismatchError on divergent session", async () => {
    const client = setupTestNode();
    const { storage } = client.addStorage();
    const { peer: serverPeer } = client.connectToSyncServer();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("a", "1", "trusting");
    await map.core.waitForSync();

    const mapId = map.id;
    const serverPeerId = serverPeer.id;

    // Block storage — next transaction syncs to server but not local storage
    const block = blockStorageWrites(storage);
    map.set("b", "2", "trusting");
    await client.node.syncManager.waitForSyncWithPeer(
      serverPeerId,
      mapId,
      5000,
    );

    client.disconnect();
    block.unblock();

    await client.restart();
    client.addStorage({ storage });

    const mapAfterRestart = (await loadCoValueOrFail(
      client.node,
      mapId,
    )) as RawCoMap;

    // Make divergent + extra tx to go past server count
    mapAfterRestart.set("c", "3", "trusting");
    mapAfterRestart.set("d", "4", "trusting");

    SyncMessagesLog.clear();
    client.connectToSyncServer();

    // Wait for the error message to appear
    await waitForCondition(() => {
      const errorMessages = SyncMessagesLog.messages.filter(
        (m) =>
          typeof m.msg === "object" &&
          "action" in m.msg &&
          m.msg.action === "error",
      );
      return errorMessages.length >= 1;
    });

    const errorMessages = SyncMessagesLog.messages.filter(
      (m) =>
        typeof m.msg === "object" &&
        "action" in m.msg &&
        m.msg.action === "error",
    );
    expect(errorMessages.length).toBe(1);

    const errorMsg = errorMessages[0]!.msg as any;
    expect(errorMsg.errorType).toBe("SignatureMismatch");
    expect(errorMsg.id).toBe(mapId);
    expect(errorMsg.content.length).toBeGreaterThan(0);
  }, 15000);

  test("server sends error only once per session (dedup)", async () => {
    const client = setupTestNode();
    const { storage } = client.addStorage();
    const { peer: serverPeer } = client.connectToSyncServer();

    const group = client.node.createGroup();
    const map = group.createMap();
    map.set("a", "1", "trusting");
    await map.core.waitForSync();

    const mapId = map.id;
    const serverPeerId = serverPeer.id;

    const block = blockStorageWrites(storage);
    map.set("b", "2", "trusting");
    await client.node.syncManager.waitForSyncWithPeer(
      serverPeerId,
      mapId,
      5000,
    );

    client.disconnect();
    block.unblock();

    await client.restart();
    client.addStorage({ storage });

    const mapAfterRestart = (await loadCoValueOrFail(
      client.node,
      mapId,
    )) as RawCoMap;
    mapAfterRestart.set("c", "3", "trusting");
    mapAfterRestart.set("d", "4", "trusting");

    SyncMessagesLog.clear();
    client.connectToSyncServer();

    await waitForCondition(() => {
      return SyncMessagesLog.messages.some(
        (m) =>
          typeof m.msg === "object" &&
          "action" in m.msg &&
          m.msg.action === "error",
      );
    });

    // Small delay to ensure no more messages arrive
    await new Promise((r) => setTimeout(r, 500));

    const errorMessages = SyncMessagesLog.messages.filter(
      (m) =>
        typeof m.msg === "object" &&
        "action" in m.msg &&
        m.msg.action === "error",
    );
    expect(errorMessages.length).toBe(1);
  }, 15000);

  test("repairs alice after crash and preserves divergent task edits", async () => {
    /**
     * Alice crashed after Jazz Cloud accepted `status="review"`
     * but before Alice persisted it locally.
     *
     * Topology
     *   Alice --------> Jazz Cloud
     *
     * Before reconnect
     *   Alice disk   : title, priority
     *   Jazz Cloud   : title, priority, status
     *
     * Expected after recovery
     *   rewritten session : title, priority, status
     *   conflict session  : assignee, archived
     *   visible map       : title, priority, status, assignee, archived
     */
    const { alice, aliceStorage } = setupRecoveryActors();
    const { mapId } = await createSharedTaskMap(alice, {
      title: "Fix login bug",
      priority: "high",
    });

    const mapAfterRestart = await crashAfterServerAckBeforeLocalPersist(
      alice,
      aliceStorage,
      mapId,
      { status: "review" },
    );

    // Create divergent local edits
    mapAfterRestart.set("assignee", "bob", "trusting");
    mapAfterRestart.set("archived", "false", "trusting");

    alice.connectToSyncServer();

    await waitForRecovery(() => {
      const content = alice.node
        .getCoValue(mapId)
        .getCurrentContent() as RawCoMap;
      return content?.get("status") === "review";
    });

    const recovered = alice.node
      .getCoValue(mapId)
      .getCurrentContent() as RawCoMap;
    expectTaskFields(recovered, {
      title: "Fix login bug",
      priority: "high",
      status: "review",
      assignee: "bob",
      archived: "false",
    });
  }, 15000);

  test("repairs alice when jazzCloud is ahead by more transactions than alice", async () => {
    /**
     * Jazz Cloud accepted both `status="review"` and `assignee="carol"`
     * but Alice's disk has neither.
     *
     * Alice makes three post-crash edits so her session is longer than
     * the server's, which triggers the SignatureMismatch error path.
     *
     * Topology
     *   Alice --------> Jazz Cloud
     *
     * Before reconnect
     *   Alice disk   : title, priority
     *   Jazz Cloud   : title, priority, status, assignee
     *
     * Expected after recovery
     *   rewritten session : title, priority, status, assignee
     *   conflict session  : archived, owner, due
     *   visible map       : title, priority, status, assignee, archived, owner, due
     */
    const { alice, aliceStorage } = setupRecoveryActors();
    const { mapId } = await createSharedTaskMap(alice, {
      title: "Fix login bug",
      priority: "high",
    });

    const mapAfterRestart = await crashAfterServerAckBeforeLocalPersist(
      alice,
      aliceStorage,
      mapId,
      { status: "review", assignee: "carol" },
    );

    // Three post-crash edits so alice's session is ahead of the server's
    mapAfterRestart.set("archived", "true", "trusting");
    mapAfterRestart.set("owner", "alice", "trusting");
    mapAfterRestart.set("due", "friday", "trusting");

    alice.connectToSyncServer();

    await waitForRecovery(() => {
      const content = alice.node
        .getCoValue(mapId)
        .getCurrentContent() as RawCoMap;
      return (
        content?.get("assignee") === "carol" &&
        content?.get("archived") === "true"
      );
    });

    const recovered = alice.node
      .getCoValue(mapId)
      .getCurrentContent() as RawCoMap;
    expectTaskFields(recovered, {
      title: "Fix login bug",
      priority: "high",
      status: "review",
      assignee: "carol",
      archived: "true",
      owner: "alice",
      due: "friday",
    });
  }, 15000);

  test("repairs alice when there are no divergent local edits to preserve", async () => {
    /**
     * Server has `status="review"` that Alice's disk lost.
     * Alice makes new (non-overlapping) edits after restart.
     *
     * Topology
     *   Alice --------> Jazz Cloud
     *
     * Before reconnect
     *   Alice disk   : (empty map)
     *   Jazz Cloud   : status
     *
     * Expected after recovery
     *   rewritten session : status
     *   conflict session  : assignee, priority
     *   visible map       : status, assignee, priority
     */
    const { alice, aliceStorage } = setupRecoveryActors();
    const { mapId } = await createSharedTaskMap(alice, {});

    const mapAfterRestart = await crashAfterServerAckBeforeLocalPersist(
      alice,
      aliceStorage,
      mapId,
      { status: "review" },
    );

    mapAfterRestart.set("assignee", "bob", "trusting");
    mapAfterRestart.set("priority", "high", "trusting");

    alice.connectToSyncServer();

    await waitForRecovery(() => {
      const content = alice.node
        .getCoValue(mapId)
        .getCurrentContent() as RawCoMap;
      return (
        content?.get("status") === "review" &&
        content?.get("assignee") === "bob"
      );
    });

    const recovered = alice.node
      .getCoValue(mapId)
      .getCurrentContent() as RawCoMap;
    expectTaskFields(recovered, {
      status: "review",
      assignee: "bob",
      priority: "high",
    });
  }, 15000);
});
