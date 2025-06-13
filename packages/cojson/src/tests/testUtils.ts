import { metrics } from "@opentelemetry/api";
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  MetricReader,
} from "@opentelemetry/sdk-metrics";
import { expect, onTestFinished, vi } from "vitest";
import { ControlledAccount, ControlledAgent } from "../coValues/account.js";
import { WasmCrypto } from "../crypto/WasmCrypto.js";
import {
  type AgentSecret,
  type CoID,
  type CoValueCore,
  type RawAccount,
  type RawCoValue,
} from "../exports.js";
import type { SessionID } from "../ids.js";
import { LocalNode } from "../localNode.js";
import { connectedPeers } from "../streamUtils.js";
import type { Peer, SyncMessage } from "../sync.js";
import { expectGroup } from "../typeUtils/expectGroup.js";
import { toSimplifiedMessages } from "./messagesTestUtils.js";

const Crypto = await WasmCrypto.create();

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
  // Connect nodes initially
  const [node1ToNode2Peer, node2ToNode1Peer] = connectedPeers(
    "node1ToNode2",
    "node2ToNode1",
    {
      peer1role: node2Role,
      peer2role: node1Role,
    },
  );

  const node1 = await LocalNode.withNewlyCreatedAccount({
    peersToLoadFrom: [node1ToNode2Peer],
    crypto: Crypto,
    creationProps: { name: "Client" },
  });

  const node2 = await LocalNode.withNewlyCreatedAccount({
    peersToLoadFrom: [node2ToNode1Peer],
    crypto: Crypto,
    creationProps: { name: "Server" },
  });

  return {
    node1,
    node2,
    node1ToNode2Peer,
    node2ToNode1Peer,
  };
}

export async function createThreeConnectedNodes(
  node1Role: Peer["role"],
  node2Role: Peer["role"],
  node3Role: Peer["role"],
) {
  const [node1ToNode2Peer, node2ToNode1Peer] = connectedPeers(
    "node1ToNode2",
    "node2ToNode1",
    {
      peer1role: node2Role,
      peer2role: node1Role,
    },
  );

  const [node1ToNode3Peer, node3ToNode1Peer] = connectedPeers(
    "node1ToNode3",
    "node3ToNode1",
    {
      peer1role: node3Role,
      peer2role: node1Role,
    },
  );

  const [node2ToNode3Peer, node3ToNode2Peer] = connectedPeers(
    "node2ToNode3",
    "node3ToNode2",
    {
      peer1role: node3Role,
      peer2role: node2Role,
    },
  );

  const node1 = await LocalNode.withNewlyCreatedAccount({
    peersToLoadFrom: [node1ToNode2Peer, node1ToNode3Peer],
    crypto: Crypto,
    creationProps: { name: "Node 1" },
  });

  const node2 = await LocalNode.withNewlyCreatedAccount({
    peersToLoadFrom: [node2ToNode1Peer, node2ToNode3Peer],
    crypto: Crypto,
    creationProps: { name: "Node 2" },
  });

  const node3 = await LocalNode.withNewlyCreatedAccount({
    peersToLoadFrom: [node3ToNode1Peer, node3ToNode2Peer],
    crypto: Crypto,
    creationProps: { name: "Node 3" },
  });

  return {
    node1,
    node2,
    node3,
    node1ToNode2Peer,
    node2ToNode1Peer,
    node1ToNode3Peer,
    node3ToNode1Peer,
    node2ToNode3Peer,
    node3ToNode2Peer,
  };
}

export function connectTwoPeers(
  a: LocalNode,
  b: LocalNode,
  aRole: "client" | "server",
  bRole: "client" | "server",
) {
  const [aAsPeer, bAsPeer] = connectedPeers(
    "peer:" + a.getCurrentAgent().id,
    "peer:" + b.getCurrentAgent().id,
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

  onTestFinished(() => {
    node.gracefulShutdown();
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
): Promise<V> {
  const value = await node.load(id);
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
  },
) {
  const push = peer.outgoing.push;
  const pushSpy = vi.spyOn(peer.outgoing, "push");

  const blockedMessages: SyncMessage[] = [];
  const blockedIds = new Set<string>();

  pushSpy.mockImplementation(async (msg) => {
    if (
      msg.action === messageType &&
      (!opts.id || msg.id === opts.id) &&
      (!opts.once || !blockedIds.has(msg.id))
    ) {
      blockedMessages.push(msg);
      blockedIds.add(msg.id);
      return Promise.resolve();
    }

    return push.call(peer.outgoing, msg);
  });

  return {
    blockedMessages,
    sendBlockedMessages: async () => {
      for (const msg of blockedMessages) {
        await push.call(peer.outgoing, msg);
      }
      blockedMessages.length = 0;
    },
    unblock: () => pushSpy.mockRestore(),
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
      id: currentSyncServer.getCurrentAgent().id,
      role: "server",
      name: opts.syncServerName,
    },
    peer2: {
      id: opts.peerId,
      role: "client",
      name: opts.ourName,
    },
  });

  currentSyncServer.syncManager.addPeer(peer2);

  return {
    peer: peer1,
    peerStateOnServer: currentSyncServer.syncManager.peers[peer2.id]!,
    peerOnServer: peer2,
  };
}

export function createMockStoragePeer(opts: {
  ourName?: string;
  peerId: string;
}) {
  const storage = createTestNode();

  const { peer1, peer2 } = connectedPeersWithMessagesTracking({
    peer1: { id: storage.getCurrentAgent().id, role: "storage" },
    peer2: {
      id: opts.peerId,
      role: "client",
      name: opts.ourName,
    },
  });

  peer1.role = "storage";
  peer1.priority = 100;

  storage.syncManager.addPeer(peer2);

  return {
    storage,
    peer: peer1,
  };
}

export function setupTestNode(
  opts: {
    isSyncServer?: boolean;
    connected?: boolean;
  } = {},
) {
  const [admin, session] = randomAgentAndSessionID();
  let node = new LocalNode(admin.agentSecret, session, Crypto);

  if (opts.isSyncServer) {
    syncServer.current = node;
  }

  function connectToSyncServer(opts?: {
    syncServerName?: string;
    ourName?: string;
    syncServer?: LocalNode;
  }) {
    const { peer, peerStateOnServer, peerOnServer } =
      getSyncServerConnectedPeer({
        peerId: node.getCurrentAgent().id,
        syncServerName: opts?.syncServerName,
        ourName: opts?.ourName,
        syncServer: opts?.syncServer,
      });

    node.syncManager.addPeer(peer);

    return {
      peerState: node.syncManager.peers[peer.id]!,
      peer: peer,
      peerStateOnServer: peerStateOnServer,
      peerOnServer: peerOnServer,
    };
  }

  function addStoragePeer(opts: { ourName?: string } = {}) {
    const { peer, storage } = createMockStoragePeer({
      peerId: node.getCurrentAgent().id,
      ourName: opts.ourName,
    });

    node.syncManager.addPeer(peer);

    return { peer, peerState: node.syncManager.peers[peer.id]!, storage };
  }

  if (opts.connected) {
    connectToSyncServer();
  }

  const ctx = {
    node,
    connectToSyncServer,
    addStoragePeer,
    restart: () => {
      node.gracefulShutdown();
      ctx.node = node = new LocalNode(admin.agentSecret, session, Crypto);

      if (opts.isSyncServer) {
        syncServer.current = node;
      }

      return node;
    },
  };

  return ctx;
}

export async function setupTestAccount(
  opts: {
    isSyncServer?: boolean;
    connected?: boolean;
  } = {},
) {
  const ctx = await LocalNode.withNewlyCreatedAccount({
    peersToLoadFrom: [],
    crypto: Crypto,
    creationProps: { name: "Client" },
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
        peerId: ctx.node.getCurrentAgent().id,
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

  function addStoragePeer(opts: { ourName?: string } = {}) {
    const { peer, storage } = createMockStoragePeer({
      peerId: ctx.node.getCurrentAgent().id,
      ourName: opts.ourName,
    });

    ctx.node.syncManager.addPeer(peer);

    return { peer, peerState: ctx.node.syncManager.peers[peer.id]!, storage };
  }

  if (opts.connected) {
    connectToSyncServer();
  }

  return {
    node: ctx.node,
    accountID: ctx.accountID,
    connectToSyncServer,
    addStoragePeer,
  };
}

export type SyncTestMessage = {
  from: string;
  to: string;
  msg: SyncMessage;
};

export function connectedPeersWithMessagesTracking(opts: {
  peer1: { id: string; role: Peer["role"]; name?: string };
  peer2: { id: string; role: Peer["role"]; name?: string };
}) {
  const [peer1, peer2] = connectedPeers(opts.peer1.id, opts.peer2.id, {
    peer1role: opts.peer1.role,
    peer2role: opts.peer2.role,
  });

  const peer1Push = peer1.outgoing.push;
  peer1.outgoing.push = (msg) => {
    SyncMessagesLog.add({
      from: opts.peer2.name ?? opts.peer2.role,
      to: opts.peer1.name ?? opts.peer1.role,
      msg,
    });
    return peer1Push.call(peer1.outgoing, msg);
  };

  const peer2Push = peer2.outgoing.push;
  peer2.outgoing.push = (msg) => {
    SyncMessagesLog.add({
      from: opts.peer1.name ?? opts.peer1.role,
      to: opts.peer2.name ?? opts.peer2.role,
      msg,
    });
    return peer2Push.call(peer2.outgoing, msg);
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
  accountCoreEntry.internalMarkMagicallyAvailable(
    accountOnTempNode.core.verified,
  );

  return new ControlledAccount(
    accountCoreEntry.getCurrentContent() as RawAccount,
    accountOnTempNode.core.node.agentSecret,
  );
}
