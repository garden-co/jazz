import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  MetricReader,
} from "@opentelemetry/sdk-metrics";
import { assert, expect, onTestFinished, vi } from "vitest";
import { ControlledAccount, ControlledAgent } from "../coValues/account.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import {
  type AgentSecret,
  AnyRawCoValue,
  type CoID,
  type CoValueCore,
  MessageChannelLike,
  MessagePortLike,
  type RawAccount,
  RawAccountID,
  RawCoMap,
  type RawCoValue,
  StorageAPI,
} from "../exports.js";
import type { RawCoID, SessionID } from "../ids.js";
import { LocalNode } from "../localNode.js";
import { connectedPeers } from "../streamUtils.js";
import type { Peer, SyncMessage, SyncWhen } from "../sync.js";
import { expectGroup } from "../typeUtils/expectGroup.js";
import { toSimplifiedMessages } from "./messagesTestUtils.js";
import { createAsyncStorage, createSyncStorage } from "./testStorage.js";
import { CoValueHeader } from "../coValueCore/verifiedState.js";
import { idforHeader } from "../coValueCore/coValueCore.js";

let Crypto = await WasmCrypto.create();

export function setCurrentTestCryptoProvider(crypto: WasmCrypto) {
  Crypto = crypto;
}

const syncServer: {
  current: undefined | LocalNode;
} = {
  current: undefined,
};

export function randomAgentAndSessionID(): [ControlledAgent, SessionID] {
  const agentSecret = Crypto.newRandomAgentSecret();

  const sessionID = Crypto.newRandomSessionID(Crypto.getAgentID(agentSecret));

  return [new ControlledAgent(agentSecret, Crypto), sessionID];
}

export function agentAndSessionIDFromSecret(
  secret: AgentSecret,
): [ControlledAgent, SessionID] {
  const sessionID = Crypto.newRandomSessionID(Crypto.getAgentID(secret));

  return [new ControlledAgent(secret, Crypto), sessionID];
}

export function nodeWithRandomAgentAndSessionID() {
  const [agent, session] = randomAgentAndSessionID();
  return new LocalNode(agent.agentSecret, session, Crypto);
}

export function createTestNode() {
  const [admin, session] = randomAgentAndSessionID();
  return new LocalNode(admin.agentSecret, session, Crypto);
}

export async function createTwoConnectedNodes(
  node1Role: Peer["role"],
  node2Role: Peer["role"],
) {
  const [node1, node2] = await createNConnectedNodes(node1Role, node2Role);

  return {
    node1: node1!,
    node2: node2!,
  };
}

export async function createThreeConnectedNodes(
  node1Role: Peer["role"],
  node2Role: Peer["role"],
  node3Role: Peer["role"],
) {
  const [node1, node2, node3] = await createNConnectedNodes(
    node1Role,
    node2Role,
    node3Role,
  );

  return {
    node1: node1!,
    node2: node2!,
    node3: node3!,
  };
}

export async function createNConnectedNodes(...nodeRoles: Peer["role"][]) {
  const nodes = await Promise.all(
    Array.from({ length: nodeRoles.length }, async (_, i) => {
      return LocalNode.withNewlyCreatedAccount({
        peers: [],
        crypto: Crypto,
        creationProps: { name: `Node ${i + 1}` },
      });
    }),
  );
  for (let i = 0; i < nodes.length; i++) {
    for (let j = i + 1; j < nodes.length; j++) {
      connectTwoPeers(
        nodes[i]!.node,
        nodes[j]!.node,
        nodeRoles[i]!,
        nodeRoles[j]!,
      );
    }
  }
  return nodes;
}

export function connectTwoPeers(
  a: LocalNode,
  b: LocalNode,
  aRole: "client" | "server",
  bRole: "client" | "server",
) {
  const [aAsPeer, bAsPeer] = connectedPeers(
    "peer:" + a.currentSessionID,
    "peer:" + b.currentSessionID,
    {
      peer1role: aRole,
      peer2role: bRole,
    },
  );

  a.syncManager.addPeer(bAsPeer);
  b.syncManager.addPeer(aAsPeer);
}

export function newGroup() {
  const [admin, sessionID] = randomAgentAndSessionID();

  const node = new LocalNode(admin.agentSecret, sessionID, Crypto);

  const groupCore = node.createCoValue({
    type: "comap",
    ruleset: { type: "group", initialAdmin: node.getCurrentAgent().id },
    meta: null,
    ...Crypto.createdNowUnique(),
  });

  const group = expectGroup(groupCore.getCurrentContent());

  group.set(node.getCurrentAgent().id, "admin", "trusting");
  expect(group.get(node.getCurrentAgent().id)).toEqual("admin");

  return { node, groupCore, admin };
}

export function groupWithTwoAdmins() {
  const { groupCore, admin, node } = newGroup();

  const otherAdmin = createAccountInNode(node);

  const group = expectGroup(groupCore.getCurrentContent());

  group.set(otherAdmin.id, "admin", "trusting");
  expect(group.get(otherAdmin.id)).toEqual("admin");

  if (group.type !== "comap") {
    throw new Error("Expected map");
  }

  expect(group.get(otherAdmin.id)).toEqual("admin");
  return { group, groupCore, admin, otherAdmin, node };
}

export function newGroupHighLevel() {
  const [admin, sessionID] = randomAgentAndSessionID();

  const node = new LocalNode(admin.agentSecret, sessionID, Crypto);

  const group = node.createGroup();

  onTestFinished(async () => {
    await node.gracefulShutdown();
  });
  return { admin, node, group };
}

export function groupWithTwoAdminsHighLevel() {
  const { admin, node, group } = newGroupHighLevel();

  const otherAdmin = createAccountInNode(node);

  group.addMember(otherAdmin, "admin");

  return { admin, node, group, otherAdmin };
}

export function shouldNotResolve<T>(
  promise: Promise<T>,
  ops: { timeout: number },
): Promise<void> {
  return new Promise((resolve, reject) => {
    promise
      .then((v) =>
        reject(
          new Error(
            "Should not have resolved, but resolved to " + JSON.stringify(v),
          ),
        ),
      )
      .catch(reject);
    setTimeout(resolve, ops.timeout);
  });
}

export function waitFor(
  callback: () => boolean | void | Promise<boolean | void>,
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

export async function loadCoValueOrFail<V extends RawCoValue>(
  node: LocalNode,
  id: CoID<V>,
  skipRetry?: boolean,
): Promise<V> {
  const value = await node.load(id, skipRetry);
  if (value === "unavailable") {
    throw new Error("CoValue not found");
  }
  return value;
}

export function blockMessageTypeOnOutgoingPeer(
  peer: Peer,
  messageType: SyncMessage["action"],
  opts: {
    id?: string;
    once?: boolean;
    matcher?: (msg: SyncMessage) => boolean;
  },
) {
  const push = peer.outgoing.push;

  const blockedMessages: SyncMessage[] = [];
  const blockedIds = new Set<string>();

  peer.outgoing.push = async (msg) => {
    if (
      typeof msg === "object" &&
      msg.action === messageType &&
      (!opts.id || msg.id === opts.id) &&
      (!opts.once || !blockedIds.has(msg.id)) &&
      (!opts.matcher || opts.matcher(msg))
    ) {
      blockedMessages.push(msg);
      blockedIds.add(msg.id);
      return Promise.resolve();
    }
    return push.call(peer.outgoing, msg);
  };

  return {
    blockedMessages,
    sendBlockedMessages: async () => {
      for (const msg of blockedMessages) {
        await push.call(peer.outgoing, msg);
      }
      blockedMessages.length = 0;
    },
    unblock: () => {
      peer.outgoing.push = push;
    },
  };
}

export function hotSleep(ms: number) {
  const before = Date.now();
  while (Date.now() < before + ms) {
    /* hot sleep */
  }
  return before;
}

/**
 * This is a test metric reader that uses an in-memory metric exporter and exposes a method to get the value of a metric given its name and attributes.
 *
 * This is useful for testing the values of metrics that are collected by the SDK.
 *
 * TODO: We may want to rethink how we access metrics (see `getMetricValue` method) to make it more flexible.
 */
class TestMetricReader extends MetricReader {
  private _exporter = new InMemoryMetricExporter(
    AggregationTemporality.CUMULATIVE,
  );

  protected onShutdown(): Promise<void> {
    throw new Error("Method not implemented.");
  }
  protected onForceFlush(): Promise<void> {
    throw new Error("Method not implemented.");
  }

  async getMetricValue(
    name: string,
    attributes: { [key: string]: string | number } = {},
  ) {
    await this.collectAndExport();
    const metric = this._exporter
      .getMetrics()[0]
      ?.scopeMetrics[0]?.metrics.find((m) => m.descriptor.name === name);

    const dp = metric?.dataPoints.find(
      (dp) => JSON.stringify(dp.attributes) === JSON.stringify(attributes),
    );

    this._exporter.reset();

    return dp?.value;
  }

  async collectAndExport(): Promise<void> {
    const result = await this.collect();
    await new Promise<void>((resolve, reject) => {
      this._exporter.export(result.resourceMetrics, (result) => {
        if (result.error != null) {
          reject(result.error);
        } else {
          resolve();
        }
      });
    });
  }
}

export function createTestMetricReader() {
  const metricReader = new TestMetricReader();
  const success = metrics.setGlobalMeterProvider(
    new MeterProvider({
      readers: [metricReader],
    }),
  );

  expect(success).toBe(true);

  return metricReader;
}

export function tearDownTestMetricReader() {
  metrics.disable();
}

export class SyncMessagesLog {
  static messages: SyncTestMessage[] = [];

  static add(message: SyncTestMessage) {
    this.messages.push(message);
  }

  static clear() {
    this.messages.length = 0;
  }

  static getMessages(coValueMapping: { [key: string]: CoValueCore }) {
    return toSimplifiedMessages(coValueMapping, SyncMessagesLog.messages);
  }

  static debugMessages(coValueMapping: { [key: string]: CoValueCore }) {
    console.log(SyncMessagesLog.getMessages(coValueMapping));
  }
}

export function getSyncServerConnectedPeer(opts: {
  syncServerName?: string;
  ourName?: string;
  syncServer?: LocalNode;
  peerId: string;
  persistent?: boolean;
}) {
  const currentSyncServer = opts?.syncServer ?? syncServer.current;

  if (!currentSyncServer) {
    throw new Error("Sync server not initialized");
  }

  if (currentSyncServer.getCurrentAgent().id === opts.peerId) {
    throw new Error("Cannot connect to self");
  }

  const { peer1, peer2 } = connectedPeersWithMessagesTracking({
    peer1: {
      id: currentSyncServer.currentSessionID,
      role: "server",
      name: opts.syncServerName,
    },
    peer2: {
      id: opts.peerId,
      role: "client",
      name: opts.ourName,
    },
    persistent: opts?.persistent,
  });

  currentSyncServer.syncManager.addPeer(peer2);

  return {
    peer: peer1,
    peerStateOnServer: currentSyncServer.syncManager.peers[peer2.id]!,
    peerOnServer: peer2,
  };
}

export const TEST_NODE_CONFIG = {
  withAsyncPeers: false,
};

export function setupTestNode(
  opts: {
    isSyncServer?: boolean;
    connected?: boolean;
    secret?: AgentSecret;
    syncWhen?: SyncWhen;
    enableFullStorageReconciliation?: boolean;
  } = {},
) {
  const [admin, session] = opts.secret
    ? agentAndSessionIDFromSecret(opts.secret)
    : randomAgentAndSessionID();

  let node = new LocalNode(
    admin.agentSecret,
    session,
    Crypto,
    opts.syncWhen,
    opts.enableFullStorageReconciliation,
  );

  if (opts.isSyncServer) {
    syncServer.current = node;
  }

  function connectToSyncServer(opts?: {
    syncServerName?: string;
    ourName?: string;
    syncServer?: LocalNode;
    persistent?: boolean;
    skipReconciliation?: boolean;
  }) {
    const { peer, peerStateOnServer, peerOnServer } =
      getSyncServerConnectedPeer({
        peerId: session,
        syncServerName: opts?.syncServerName,
        ourName: opts?.ourName,
        syncServer: opts?.syncServer,
        persistent: opts?.persistent,
      });

    node.syncManager.addPeer(peer, opts?.skipReconciliation);

    return {
      peerState: node.syncManager.peers[peer.id]!,
      peer: peer,
      peerStateOnServer: peerStateOnServer,
      peerOnServer: peerOnServer,
    };
  }

  function addStorage(opts: { ourName?: string; storage?: StorageAPI } = {}) {
    const storage =
      opts.storage ??
      createSyncStorage({
        nodeName: opts.ourName ?? "client",
        storageName: "storage",
      });
    node.setStorage(storage);

    return { storage };
  }

  async function addAsyncStorage(
    opts: { ourName?: string; filename?: string; storageName?: string } = {},
  ) {
    const storage = await createAsyncStorage({
      nodeName: opts.ourName ?? "client",
      storageName: opts.storageName ?? "storage",
      filename: opts.filename,
    });
    node.setStorage(storage);

    return { storage };
  }

  if (opts.connected) {
    connectToSyncServer();
  }

  onTestFinished(async () => {
    await node.gracefulShutdown();
  });

  const ctx = {
    node,
    connectToSyncServer,
    addStorage,
    addAsyncStorage,
    restart: async () => {
      await node.gracefulShutdown();
      ctx.node = node = new LocalNode(
        admin.agentSecret,
        session,
        Crypto,
        opts.syncWhen,
        opts.enableFullStorageReconciliation,
      );

      if (opts.isSyncServer) {
        syncServer.current = node;
      }

      return node;
    },
    spawnNewSession: () => {
      return setupTestNode({
        secret: node.agentSecret,
        connected: opts.connected,
        isSyncServer: opts.isSyncServer,
        enableFullStorageReconciliation: opts.enableFullStorageReconciliation,
      });
    },
    disconnect: () => {
      const allPeers = Object.values(node.syncManager.peers);
      allPeers.forEach((peer) => {
        peer.gracefulShutdown();
      });
      node.syncManager.peers = {};
    },
  };

  return ctx;
}

export async function setupTestAccount(
  opts: {
    isSyncServer?: boolean;
    connected?: boolean;
    storage?: StorageAPI;
    accountID?: RawAccountID;
    accountSecret?: AgentSecret;
  } = {},
) {
  const ctx =
    opts.accountSecret && opts.accountID
      ? {
          node: await LocalNode.withLoadedAccount({
            peers: [
              getSyncServerConnectedPeer({
                peerId: opts.accountID,
              }).peer,
            ],
            crypto: Crypto,
            storage: opts.storage,
            accountID: opts.accountID,
            accountSecret: opts.accountSecret,
            sessionID: Crypto.newRandomSessionID(opts.accountID),
          }),
          accountID: opts.accountID,
          accountSecret: opts.accountSecret,
        }
      : await LocalNode.withNewlyCreatedAccount({
          peers: [],
          crypto: Crypto,
          creationProps: { name: "Client" },
          storage: opts.storage,
        });

  if (opts.isSyncServer) {
    syncServer.current = ctx.node;
  }

  function connectToSyncServer(opts?: {
    syncServerName?: string;
    ourName?: string;
    syncServer?: LocalNode;
  }) {
    const { peer, peerStateOnServer, peerOnServer } =
      getSyncServerConnectedPeer({
        peerId: ctx.node.currentSessionID,
        syncServerName: opts?.syncServerName,
        ourName: opts?.ourName,
        syncServer: opts?.syncServer,
      });

    ctx.node.syncManager.addPeer(peer);

    function getCurrentPeerState() {
      return ctx.node.syncManager.peers[peer.id]!;
    }

    return {
      peerState: getCurrentPeerState(),
      peer,
      peerStateOnServer: peerStateOnServer,
      peerOnServer: peerOnServer,
      getCurrentPeerState,
    };
  }

  function addStorage(opts: { ourName?: string; storage?: StorageAPI } = {}) {
    const storage =
      opts.storage ??
      createSyncStorage({
        nodeName: opts.ourName ?? "client",
        storageName: "storage",
      });
    ctx.node.setStorage(storage);

    return { storage };
  }

  async function addAsyncStorage(
    opts: { ourName?: string; storageName?: string } = {},
  ) {
    const storage = await createAsyncStorage({
      nodeName: opts.ourName ?? "client",
      storageName: opts.storageName ?? "storage",
    });
    ctx.node.setStorage(storage);

    return { storage };
  }

  if (opts.connected) {
    connectToSyncServer();
  }

  onTestFinished(async () => {
    await ctx.node.gracefulShutdown();
  });

  const account = ctx.node
    .getCoValue(ctx.accountID)
    .getCurrentContent() as RawAccount;

  return {
    node: ctx.node,
    accountID: ctx.accountID,
    account,
    connectToSyncServer,
    addStorage,
    addAsyncStorage,
    spawnNewSession: () => {
      return setupTestAccount({
        accountID: ctx.accountID,
        accountSecret: ctx.accountSecret,
        connected: true,
      });
    },
    disconnect: () => {
      const allPeers = ctx.node.syncManager.getPeers(ctx.accountID);
      allPeers.forEach((peer) => {
        peer.gracefulShutdown();
      });
      ctx.node.syncManager.peers = {};
    },
  };
}

export type LazyLoadMessage = {
  action: "lazyLoad";
  id: RawCoID;
};

export type LazyLoadResultMessage = {
  action: "lazyLoadResult";
  id: RawCoID;
  header: boolean;
  sessions: { [sessionID: string]: number };
};

export type SyncTestMessage = {
  from: string;
  to: string;
  msg: SyncMessage | LazyLoadMessage | LazyLoadResultMessage;
};

export function connectedPeersWithMessagesTracking(opts: {
  peer1: { id: string; role: Peer["role"]; name?: string };
  peer2: { id: string; role: Peer["role"]; name?: string };
  persistent?: boolean;
}) {
  const [peer1, peer2] = connectedPeers(opts.peer1.id, opts.peer2.id, {
    peer1role: opts.peer1.role,
    peer2role: opts.peer2.role,
    persistent: opts.persistent,
  });

  // If the persistent option is not provided, we default to true for the server and false for the client
  // Trying to mimic the real world behavior of the sync server
  if (opts.persistent === undefined) {
    peer1.persistent = opts.peer1.role === "server";

    peer2.persistent = opts.peer2.role === "server";
  }

  const peer1Push = peer1.outgoing.push;
  peer1.outgoing.push = (msg) => {
    if (typeof msg !== "string") {
      SyncMessagesLog.add({
        from: opts.peer2.name ?? opts.peer2.role,
        to: opts.peer1.name ?? opts.peer1.role,
        msg,
      });
    }

    if (!TEST_NODE_CONFIG.withAsyncPeers) {
      peer1Push.call(peer1.outgoing, msg);
    } else {
      // Simulate the async nature of the real push
      setTimeout(() => {
        peer1Push.call(peer1.outgoing, msg);
      }, 0);
    }
  };

  const peer2Push = peer2.outgoing.push;
  peer2.outgoing.push = (msg) => {
    if (typeof msg !== "string") {
      SyncMessagesLog.add({
        from: opts.peer1.name ?? opts.peer1.role,
        to: opts.peer2.name ?? opts.peer2.role,
        msg,
      });
    }

    if (!TEST_NODE_CONFIG.withAsyncPeers) {
      peer2Push.call(peer2.outgoing, msg);
    } else {
      // Simulate the async nature of the real push
      setTimeout(() => {
        peer2Push.call(peer2.outgoing, msg);
      }, 0);
    }
  };

  return {
    peer1,
    peer2,
  };
}

export function createAccountInNode(node: LocalNode) {
  const accountOnTempNode = LocalNode.internalCreateAccount({
    crypto: node.crypto,
  });

  const accountCoreEntry = node.getCoValue(accountOnTempNode.id);

  const content = accountOnTempNode.core.newContentSince(undefined)?.[0]!;

  node.syncManager.handleNewContent(content, "import");

  return new ControlledAccount(
    accountCoreEntry.getCurrentContent() as RawAccount,
    accountOnTempNode.core.node.agentSecret,
  );
}

export function createUnloadedCoValue(
  node: LocalNode,
  type: AnyRawCoValue["type"] = "comap",
) {
  const header = {
    type,
    ruleset: { type: "ownedByGroup", group: node.getCurrentAccountOrAgentID() },
    meta: null,
    ...node.crypto.createdNowUnique(),
  } as CoValueHeader;

  const id = idforHeader(header, node.crypto);

  const state = node.getCoValue(id);

  return { coValue: state, id, header };
}

export function fillCoMapWithLargeData(map: RawCoMap) {
  const dataSize = 1 * 1024 * 200;
  const chunkSize = 1024; // 1KB chunks
  const chunks = dataSize / chunkSize;

  const value = Buffer.alloc(chunkSize, `value$`).toString("base64");

  for (let i = 0; i < chunks; i++) {
    const key = `key${i}`;
    map.set(key, value, "trusting");
  }

  return map;
}

export function importContentIntoNode(
  coValue: CoValueCore,
  node: LocalNode,
  chunks?: number,
) {
  const content = coValue.newContentSince(undefined);
  assert(content);
  for (const [i, chunk] of content.entries()) {
    if (chunks && i >= chunks) {
      break;
    }
    node.syncManager.handleNewContent(chunk, "import");
  }
}

// ============================================================================
// MessageChannel Test Helpers
// ============================================================================

/**
 * Type guard to check if a message is a SyncMessage.
 */
export function isSyncMessage(msg: unknown): msg is SyncMessage {
  return (
    typeof msg === "object" &&
    msg !== null &&
    "action" in msg &&
    typeof (msg as { action: unknown }).action === "string"
  );
}

/**
 * Creates a MessageChannel that logs all sync messages exchanged between ports.
 * Similar to connectedPeersWithMessagesTracking but for MessageChannel.
 */
export function createTrackedMessageChannel(opts: {
  port1Name?: string;
  port2Name?: string;
}) {
  const { port1, port2 } = new MessageChannel();
  const port1Name = opts.port1Name ?? "port1";
  const port2Name = opts.port2Name ?? "port2";

  // Wrap port1.postMessage to log messages
  const originalPort1PostMessage = port1.postMessage.bind(port1);
  port1.postMessage = (message, transfer) => {
    if (isSyncMessage(message)) {
      SyncMessagesLog.add({
        from: port1Name,
        to: port2Name,
        msg: message,
      });
    }

    originalPort1PostMessage(message, transfer);
  };

  // Wrap port2.postMessage to log messages
  const originalPort2PostMessage = port2.postMessage.bind(port2);
  port2.postMessage = (message, transfer) => {
    if (isSyncMessage(message)) {
      SyncMessagesLog.add({
        from: port2Name,
        to: port1Name,
        msg: message,
      });
    }

    originalPort2PostMessage(message, transfer);
  };

  return { port1, port2 };
}

/**
 * Creates a mock worker target that simulates receiving a port
 * and calling a callback with the received port (simulating a connection handshake).
 */
export function createMockWorkerWithAccept(
  onPortReceived: (port: MessagePortLike) => Promise<void>,
) {
  return {
    postMessage: vi.fn().mockImplementation((data, transfer) => {
      if (data?.type === "jazz:port" && transfer?.[0]) {
        const port = transfer[0] as MessagePortLike;
        // Simulate the worker receiving the port and calling accept
        onPortReceived(port);
      }
    }),
  };
}
