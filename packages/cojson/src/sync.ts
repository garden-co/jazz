import { ValueType, metrics } from "@opentelemetry/api";
import { CoValueHeader, Transaction } from "./coValueCore.js";
import { CoValueCore } from "./coValueCore.js";
import { Signature } from "./crypto/crypto.js";
import { RawCoID, SessionID } from "./ids.js";
import { LocalNode } from "./localNode.js";
import { logger } from "./logger.js";
import { CoValuePriority } from "./priority.js";

export type CoValueKnownState = {
  id: RawCoID;
  header: boolean;
  sessions: { [sessionID: SessionID]: number };
};

export function emptyKnownState(id: RawCoID): CoValueKnownState {
  return {
    id,
    header: false,
    sessions: {},
  };
}

export function knownStateIn(msg: LoadMessage | KnownStateMessage) {
  return {
    id: msg.id,
    header: msg.header,
    sessions: msg.sessions,
  };
}

function getErrorMessage(e: unknown) {
  return e instanceof Error ? e.message : "Unknown error";
}

export type SyncMessage =
  | LoadMessage
  | KnownStateMessage
  | NewContentMessage
  | DoneMessage;

export type LoadMessage = {
  action: "load";
} & CoValueKnownState;

export type KnownStateMessage = {
  action: "known";
  asDependencyOf?: RawCoID;
  isCorrection?: boolean;
} & CoValueKnownState;

export type NewContentMessage = {
  action: "content";
  id: RawCoID;
  header?: CoValueHeader;
  priority: CoValuePriority;
  new: {
    [sessionID: SessionID]: SessionNewContent;
  };
};

export type SessionNewContent = {
  after: number;
  newTransactions: Transaction[];
  lastSignature: Signature;
};
export type DoneMessage = {
  action: "done";
  id: RawCoID;
};

export type PeerID = string;

export type DisconnectedError = "Disconnected";

export type PingTimeoutError = "PingTimeout";

export type IncomingSyncStream = AsyncIterable<
  SyncMessage | DisconnectedError | PingTimeoutError
>;
export type OutgoingSyncQueue = {
  push: (msg: SyncMessage) => Promise<unknown>;
  close: () => void;
};

export interface Peer {
  id: PeerID;
  incoming: IncomingSyncStream;
  outgoing: OutgoingSyncQueue;
  role: "peer" | "server" | "client" | "storage";
  priority?: number;
  crashOnClose: boolean;
  deletePeerStateOnClose?: boolean;
}

export function combinedKnownStates(
  stateA: CoValueKnownState,
  stateB: CoValueKnownState,
): CoValueKnownState {
  const sessionStates: CoValueKnownState["sessions"] = {};

  const allSessions = new Set([
    ...Object.keys(stateA.sessions),
    ...Object.keys(stateB.sessions),
  ] as SessionID[]);

  for (const sessionID of allSessions) {
    const stateAValue = stateA.sessions[sessionID];
    const stateBValue = stateB.sessions[sessionID];

    sessionStates[sessionID] = Math.max(stateAValue || 0, stateBValue || 0);
  }

  return {
    id: stateA.id,
    header: stateA.header || stateB.header,
    sessions: sessionStates,
  };
}

export class SyncManager {
  peers: { [key: PeerID]: Peer } = {};
  local: LocalNode;

  peersCounter = metrics.getMeter("cojson").createUpDownCounter("jazz.peers", {
    description: "Amount of connected peers",
    valueType: ValueType.INT,
    unit: "peer",
  });

  constructor(local: LocalNode) {
    this.local = local;
  }

  peersInPriorityOrder(): Peer[] {
    return Object.values(this.peers).sort((a, b) => {
      const aPriority = a.priority || 0;
      const bPriority = b.priority || 0;

      return bPriority - aPriority;
    });
  }

  getPeers(): Peer[] {
    return Object.values(this.peers);
  }

  getServerAndStoragePeers(excludePeerId?: PeerID): Peer[] {
    return this.peersInPriorityOrder().filter(
      (peer) =>
        (peer.role === "server" || peer.role === "storage") &&
        peer.id !== excludePeerId,
    );
  }

  addPeer(peer: Peer) {
    const prevPeer = this.peers[peer.id];
    this.peers[peer.id] = peer;

    if (prevPeer && !prevPeer.closed) {
      prevPeer.gracefulShutdown();
    }

    this.peersCounter.add(1, { role: peer.role });

    const dispatchMessages = async () => {
      for await (const msg of peer.incoming) {
        if (msg === "Disconnected") {
          return;
        }
        if (msg === "PingTimeout") {
          logger.error("Ping timeout from peer", {
            peerId: peer.id,
            peerRole: peer.role,
          });
          return;
        }

        this.local.coValuesStore
          .getOrCreateEmpty(msg.id)
          .incomingMessages.push({
            peer,
            message: msg,
          });
      }
    };

    dispatchMessages()
      .then(() => {
        if (peer.crashOnClose) {
          logger.error("Unexepcted close from peer", {
            peerId: peer.id,
            peerRole: peer.role,
          });
          this.local.crashed = new Error("Unexpected close from peer");
          throw new Error("Unexpected close from peer");
        }
      })
      .catch((e) => {
        logger.error("Error processing messages from peer", {
          err: e,
          peerId: peer.id,
          peerRole: peer.role,
        });
        if (peer.crashOnClose) {
          this.local.crashed = e;
          throw new Error(e);
        }
      })
      .finally(() => {
        const state = this.peers[peer.id];
        state?.gracefulShutdown();
        this.peersCounter.add(-1, { role: peer.role });

        if (peer.deletePeerStateOnClose) {
          delete this.peers[peer.id];
        }
      });
  }

  async waitForSyncWithPeer(peerId: PeerID, id: RawCoID, timeout: number) {
    const { syncState } = this;
    const currentSyncState = syncState.getCurrentSyncState(peerId, id);

    const isTheConditionAlreadyMet = currentSyncState.uploaded;

    if (isTheConditionAlreadyMet) {
      return true;
    }

    return new Promise((resolve, reject) => {
      const unsubscribe = this.syncState.subscribeToPeerUpdates(
        peerId,
        (knownState, syncState) => {
          if (syncState.uploaded && knownState.id === id) {
            resolve(true);
            unsubscribe?.();
            clearTimeout(timeoutId);
          }
        },
      );

      const timeoutId = setTimeout(() => {
        reject(new Error(`Timeout waiting for sync on ${peerId}/${id}`));
        unsubscribe?.();
      }, timeout);
    });
  }

  async waitForSync(id: RawCoID, timeout = 30_000) {
    const peers = this.getPeers();

    return Promise.all(
      peers.map((peer) => this.waitForSyncWithPeer(peer.id, id, timeout)),
    );
  }

  async waitForAllCoValuesSync(timeout = 60_000) {
    const coValues = this.local.coValuesStore.getValues();
    const validCoValues = Array.from(coValues).filter(
      (coValue) =>
        coValue.state.type === "available" || coValue.state.type === "loading",
    );

    return Promise.all(
      validCoValues.map((coValue) => this.waitForSync(coValue.id, timeout)),
    );
  }

  gracefulShutdown() {
    for (const peer of Object.values(this.peers)) {
      peer.gracefulShutdown();
    }
  }
}
