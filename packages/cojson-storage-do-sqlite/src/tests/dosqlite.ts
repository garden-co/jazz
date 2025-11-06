import "jazz-tools/load-edge-wasm";
import { env, DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import {
  LocalNode,
  RawCoID,
  StorageApiSync,
  SyncMessage,
  cojsonInternals,
} from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";

import { toSimplifiedMessages } from "./messagesTestUtils.js";
import { getDurableObjectSqlStorage } from "../index.js";

const Crypto = await WasmCrypto.create();

// Adopted from cojson-storage-sqlite/src/tests/testUtils.js
export function trackMessages() {
  const messages: {
    from: "client" | "server" | "storage";
    msg: SyncMessage;
  }[] = [];

  const originalLoad = StorageApiSync.prototype.load;
  const originalStore = StorageApiSync.prototype.store;

  StorageApiSync.prototype.load = async function (id, callback, done) {
    messages.push({
      from: "client",
      msg: {
        action: "load",
        id: id as RawCoID,
        header: false,
        sessions: {},
      },
    });
    return originalLoad.call(
      this,
      id,
      (msg) => {
        messages.push({
          from: "storage",
          msg,
        });
        callback(msg);
      },
      done,
    );
  };

  StorageApiSync.prototype.store = function (data, correctionCallback) {
    messages.push({
      from: "client",
      msg: data,
    });

    return originalStore.call(this, data, (msg) => {
      messages.push({
        from: "storage",
        msg: {
          action: "known",
          isCorrection: true,
          ...msg,
        },
      });

      const correctionMessages = correctionCallback(msg);

      if (correctionMessages) {
        for (const msg of correctionMessages) {
          messages.push({
            from: "client",
            msg,
          });
        }
      }

      return correctionMessages;
    });
  };

  const restore = () => {
    StorageApiSync.prototype.load = originalLoad;
    StorageApiSync.prototype.store = originalStore;
    messages.length = 0;
  };

  return {
    messages,
    restore,
  };
}

function waitFor(
  callback: () => boolean | undefined | Promise<boolean | undefined>,
) {
  return new Promise<void>((resolve, reject) => {
    const checkPassed = async () => {
      try {
        return { ok: await callback(), error: null };
      } catch (error) {
        return { ok: false, error };
      }
    };

    let retries = 0;

    const interval = setInterval(async () => {
      const { ok, error } = await checkPassed();

      if (ok !== false) {
        clearInterval(interval);
        resolve();
      }

      if (++retries > 10) {
        clearInterval(interval);
        reject(error);
      }
    }, 100);
  });
}

/**
 * Due to issues with @cloudflare/vitest-pool-workers and loading wasm modules
 * implementing the test logic in the durable object and running full server
 * with wrangler.
 *
 */
export class DoSqlStoreTest extends DurableObject {
  /**
   * Test case from cojson-storage-sqlite: "should sync and load data from storage"
   */
  async storeLoadTest() {
    const agentSecret = Crypto.newRandomAgentSecret();

    const node1 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );
    const node1Sync = trackMessages();
    const storage1 = getDurableObjectSqlStorage(this.ctx.storage);
    node1.setStorage(storage1);
    const group = node1.createGroup();

    const map = group.createMap();

    map.set("hello", "world");

    await new Promise((resolve) => setTimeout(resolve, 200));

    const messageRes1 = toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      node1Sync.messages,
    );

    node1Sync.restore();

    const node2 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );

    const node2Sync = trackMessages();
    const storage2 = getDurableObjectSqlStorage(this.ctx.storage);
    node2.setStorage(storage2);

    const map2 = await node2.load(map.id);
    if (map2 === "unavailable") {
      throw new Error("Map is unavailable");
    }

    const messageRes2 = toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      node2Sync.messages,
    );

    node2Sync.restore();

    return {
      messageRes1,
      messageRes2,

      map2Hello: map2.get("hello"),
    };
  }

  /**
   * Test case from cojson-storage-sqlite: "should send an empty content message if there is no content"
   */
  async storeLoadEmptyTest() {
    const agentSecret = Crypto.newRandomAgentSecret();

    const node1 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );

    const node1Sync = trackMessages();
    const storage1 = getDurableObjectSqlStorage(this.ctx.storage);
    node1.setStorage(storage1);

    const group = node1.createGroup();

    const map = group.createMap();

    await new Promise((resolve) => setTimeout(resolve, 200));

    const messageRes1 = toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      node1Sync.messages,
    );

    node1Sync.restore();

    const node2 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );

    const node2Sync = trackMessages();
    const storage2 = getDurableObjectSqlStorage(this.ctx.storage);
    node2.setStorage(storage2);

    const map2 = await node2.load(map.id);
    if (map2 === "unavailable") {
      throw new Error("Map is unavailable");
    }

    const messageRes2 = toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      node2Sync.messages,
    );

    node2Sync.restore();
    return {
      messageRes1,
      messageRes2,
    };
  }

  /**
   * Test case from cojson-storage-sqlite: "should load dependencies correctly (group inheritance)"
   */
  async loadDependenciesTest() {
    const agentSecret = Crypto.newRandomAgentSecret();

    const node1 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );

    const node1Sync = trackMessages();

    const storage1 = getDurableObjectSqlStorage(this.ctx.storage);
    node1.setStorage(storage1);

    const group = node1.createGroup();
    const parentGroup = node1.createGroup();

    group.extend(parentGroup);

    const map = group.createMap();

    map.set("hello", "world");

    await new Promise((resolve) => setTimeout(resolve, 200));

    const messageRes1 = toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
        ParentGroup: parentGroup.core,
      },
      node1Sync.messages,
    );
    node1Sync.restore();

    const node2 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );

    const node2Sync = trackMessages();
    const storage2 = getDurableObjectSqlStorage(this.ctx.storage);
    node2.setStorage(storage2);

    await node2.load(map.id);

    const mapLoaded = !!node2.expectCoValueLoaded(map.id);
    const groupLoaded = !!node2.expectCoValueLoaded(group.id);
    const parentGroupLoaded = !!node2.expectCoValueLoaded(parentGroup.id);

    const messageRes2 = toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
        ParentGroup: parentGroup.core,
      },
      node2Sync.messages,
    );
    return {
      messageRes1,
      messageRes2,
      mapLoaded,
      groupLoaded,
      parentGroupLoaded,
    };
  }

  /**
   * Test case from cojson-storage-sqlite: "should not send the same dependency value twice"
   */
  async duplicateDependencyTest() {
    const agentSecret = Crypto.newRandomAgentSecret();

    const node1 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );

    const node1Sync = trackMessages();
    const storage1 = getDurableObjectSqlStorage(this.ctx.storage);
    node1.setStorage(storage1);

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

    const storage2 = getDurableObjectSqlStorage(this.ctx.storage);
    node2.setStorage(storage2);

    await node2.load(map.id);
    await node2.load(mapFromParent.id);

    const mapLoaded = !!node2.expectCoValueLoaded(map.id);
    const mapFromParentLoaded = !!node2.expectCoValueLoaded(mapFromParent.id);
    const groupLoaded = !!node2.expectCoValueLoaded(group.id);
    const parentGroupLoaded = !!node2.expectCoValueLoaded(parentGroup.id);

    const messageRes1 = toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
        ParentGroup: parentGroup.core,
        MapFromParent: mapFromParent.core,
      },
      node2Sync.messages,
    );

    return {
      messageRes1,
      mapLoaded,
      mapFromParentLoaded,
      groupLoaded,
      parentGroupLoaded,
    };
  }

  /**
   * Test case from cojson-storage-sqlite: "should recover from data loss"
   */
  async dataLossRecoverTest() {
    const agentSecret = Crypto.newRandomAgentSecret();

    const node1 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );

    const node1Sync = trackMessages();
    const storage1 = getDurableObjectSqlStorage(this.ctx.storage);
    node1.setStorage(storage1);

    const group = node1.createGroup();

    const map = group.createMap();

    map.set("0", 0);

    await new Promise((resolve) => setTimeout(resolve, 200));

    const originalStore = StorageApiSync.prototype.store;
    StorageApiSync.prototype.store = () => false;

    map.set("1", 1);
    map.set("2", 2);

    await new Promise((resolve) => setTimeout(resolve, 200));

    StorageApiSync.prototype.store = originalStore;

    map.set("3", 3);

    await new Promise((resolve) => setTimeout(resolve, 200));

    const messageRes1 = toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      node1Sync.messages,
    );

    node1Sync.restore();

    const node2 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );

    const node2Sync = trackMessages();
    const storage2 = getDurableObjectSqlStorage(this.ctx.storage);
    node2.setStorage(storage2);

    const map2 = await node2.load(map.id);

    if (map2 === "unavailable") {
      throw new Error("Map is unavailable");
    }

    const mapContent = map2.toJSON();

    const messageRes2 = toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      node2Sync.messages,
    );

    return {
      messageRes1,
      messageRes2,
      mapContent,
    };
  }

  /**
   * Test case from cojson-storage-sqlite: "should recover missing dependencies from storage"
   */
  async recoverMissingDependenciesTest() {
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

    // manual mock
    const originalStore = StorageApiSync.prototype.store;
    StorageApiSync.prototype.store = function (data, correctionCallback) {
      if ([group.core.id, account.core.id as string].includes(data.id)) {
        return false;
      }
      return originalStore.call(this, data, correctionCallback);
    };

    const storage1 = getDurableObjectSqlStorage(this.ctx.storage);
    node1.setStorage(storage1);

    const group = node1.createGroup();
    group.addMember("everyone", "writer");

    const map = group.createMap();

    map.set("0", 0);

    StorageApiSync.prototype.store = originalStore;

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

    const storage2 = getDurableObjectSqlStorage(this.ctx.storage);
    node2.setStorage(storage2);

    const map2 = await node2.load(map.id);

    if (map2 === "unavailable") {
      throw new Error("Map is unavailable");
    }

    const mapContent = map2.toJSON();

    return {
      mapContent,
    };
  }

  /**
   * Test case from cojson-storage-sqlite: "should sync multiple sessions in a single content message"
   */
  async multipleSessionsSingleContentMessageTest() {
    const agentSecret = Crypto.newRandomAgentSecret();

    const node1 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );

    const storage1 = getDurableObjectSqlStorage(this.ctx.storage);
    node1.setStorage(storage1);

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

    const storage2 = getDurableObjectSqlStorage(this.ctx.storage);
    node2.setStorage(storage2);

    const map2 = await node2.load(map.id);
    if (map2 === "unavailable") {
      throw new Error("Map is unavailable");
    }

    const map2Hello = map2.get("hello");

    map2.set("hello", "world2");

    await map2.core.waitForSync();

    node2.gracefulShutdown();

    const node3 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );

    const node3Sync = trackMessages();

    const storage3 = getDurableObjectSqlStorage(this.ctx.storage);
    node3.setStorage(storage3);

    const map3 = await node3.load(map.id);
    if (map3 === "unavailable") {
      throw new Error("Map is unavailable");
    }

    const map3Hello = map3.get("hello");

    const messageRes1 = toSimplifiedMessages(
      {
        Map: map.core,
        Group: group.core,
      },
      node3Sync.messages,
    );
    node3Sync.restore();

    return {
      messageRes1,
      map2Hello,
      map3Hello,
    };
  }

  /**
   * Test case from cojson-storage-sqlite: "large coValue upload streaming"
   */
  async largeCoValueUploadTest() {
    const agentSecret = Crypto.newRandomAgentSecret();

    const node1 = new LocalNode(
      agentSecret,
      Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret)),
      Crypto,
    );

    const storage1 = getDurableObjectSqlStorage(this.ctx.storage);
    node1.setStorage(storage1);

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

    const storage2 = getDurableObjectSqlStorage(this.ctx.storage);
    node2.setStorage(storage2);

    const largeMapOnNode2 = await node2.load(largeMap.id);

    if (largeMapOnNode2 === "unavailable") {
      throw new Error("Map is unavailable");
    }

    await waitFor(() => {
      if (
        JSON.stringify(largeMapOnNode2.core.knownState()) !==
        JSON.stringify(largeMap.core.knownState())
      ) {
        throw new Error("Map states are not equal");
      }
      return true;
    });

    const messageRes1 = toSimplifiedMessages(
      {
        Map: largeMap.core,
        Group: group.core,
      },
      node2Sync.messages,
    );

    return {
      messageRes1,
    };
  }
}

export default class EntryPoint extends WorkerEntrypoint {
  async fetch(request: Request) {
    if (request.method !== "POST") {
      return new Response("Method not allowed", { status: 405 });
    }
    const path = new URL(request.url).pathname;
    const body = await request.json();
    if (
      !(
        typeof body === "object" &&
        body !== null &&
        "doId" in body &&
        typeof body.doId === "string"
      )
    ) {
      throw new Error("Invalid request body");
    }

    const stub = env.DoSqlStoreTest.getByName(
      body.doId,
    ) as DurableObjectStub<DoSqlStoreTest>;

    if (path === "/sync-and-load") {
      const res = await stub.storeLoadTest();
      // @ts-expect-error ts2589 the type initiation is too deep
      return Response.json(res);
    } else if (path === "/sync-and-load-empty") {
      const res = await stub.storeLoadEmptyTest();
      return Response.json(res);
    } else if (path === "/group-load") {
      const res = await stub.loadDependenciesTest();
      return Response.json(res);
    } else if (path === "/group-load-duplicate") {
      const res = await stub.duplicateDependencyTest();
      return Response.json(res);
    } else if (path === "/data-loss-recovery") {
      const res = await stub.dataLossRecoverTest();
      return Response.json(res);
    } else if (path === "/missing-dependency-recovery") {
      const res = await stub.recoverMissingDependenciesTest();
      return Response.json(res);
    } else if (path === "/multiple-sessions") {
      const res = await stub.multipleSessionsSingleContentMessageTest();
      return Response.json(res);
    } else if (path === "/large-covalue-upload") {
      const res = await stub.largeCoValueUploadTest();
      return Response.json(res);
    } else {
      return new Response("Invalid test method", { status: 400 });
    }
  }
}
