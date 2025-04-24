import { Histogram, ValueType, metrics } from "@opentelemetry/api";
import { PeerState } from "./PeerState.js";
import { SyncStateManager } from "./SyncStateManager.js";
import { CoValueHeader, Transaction } from "./coValueCore.js";
import { CoValueCore } from "./coValueCore.js";
import { CoValueState } from "./coValueState.js";
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
  isCorrection?: boolean;
  asDependencyOf?: RawCoID;
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
  peers: { [key: PeerID]: PeerState } = {};
  local: LocalNode;
  requestedSyncs: {
    [id: RawCoID]:
      | { done: Promise<void>; nRequestsThisTick: number }
      | undefined;
  } = {};

  peersCounter = metrics.getMeter("cojson").createUpDownCounter("jazz.peers", {
    description: "Amount of connected peers",
    valueType: ValueType.INT,
    unit: "peer",
  });
  private transactionsSizeHistogram: Histogram;

  constructor(local: LocalNode) {
    this.local = local;
    this.syncState = new SyncStateManager(this);

    this.transactionsSizeHistogram = metrics
      .getMeter("cojson")
      .createHistogram("jazz.transactions.size", {
        description: "The size of transactions in a covalue",
        unit: "bytes",
        valueType: ValueType.INT,
      });
  }

  syncState: SyncStateManager;

  peersInPriorityOrder(): PeerState[] {
    return Object.values(this.peers).sort((a, b) => {
      const aPriority = a.priority || 0;
      const bPriority = b.priority || 0;

      return bPriority - aPriority;
    });
  }

  getPeers(): PeerState[] {
    return Object.values(this.peers);
  }

  getServerAndStoragePeers(excludePeerId?: PeerID): PeerState[] {
    return this.peersInPriorityOrder().filter(
      (peer) =>
        peer.isServerOrStoragePeer() &&
        peer.id !== excludePeerId &&
        !peer.closed,
    );
  }

  async handleSyncMessage(msg: SyncMessage, peer: PeerState) {
    if (this.local.coValuesStore.get(msg.id).isErroredInPeer(peer.id)) {
      logger.warn(
        `Skipping message ${msg.action} on errored coValue ${msg.id} from peer ${peer.id}`,
      );
      return;
    } else if (msg.id === undefined) {
      logger.info("Received sync message with undefined id", {
        msg,
      });
      return;
    } else if (!msg.id.startsWith("co_z")) {
      logger.info("Received sync message with invalid id", {
        msg,
      });
      return;
    }

    // TODO: validate
    switch (msg.action) {
      case "load":
        return await this.handleLoad(msg, peer);
      case "known":
        if (msg.isCorrection) {
          return await this.handleCorrection(msg, peer);
        } else {
          return await this.handleKnownState(msg, peer);
        }
      case "content":
        // await new Promise<void>((resolve) => setTimeout(resolve, 0));
        return await this.handleNewContent(msg, peer);
      case "done":
        return await this.handleUnsubscribe(msg);
      default:
        throw new Error(
          `Unknown message type ${(msg as { action: "string" }).action}`,
        );
    }
  }

  async sendNewContentIncludingDependencies(id: RawCoID, peer: PeerState) {
    const coValue = this.local.expectCoValueLoaded(id);

    await Promise.all(
      coValue
        .getDependedOnCoValues()
        .map((id) => this.sendNewContentIncludingDependencies(id, peer)),
    );

    const newContentPieces = coValue.newContentSince(
      peer.optimisticKnownStates.get(id),
    );

    if (newContentPieces) {
      for (const piece of newContentPieces) {
        this.trySendToPeer(peer, piece).catch((e: unknown) => {
          logger.error("Error sending content piece", { err: e });
        });
      }

      peer.toldKnownState.add(id);
      peer.combineOptimisticWith(id, coValue.knownState());
    } else if (!peer.toldKnownState.has(id)) {
      this.trySendToPeer(peer, {
        action: "known",
        ...coValue.knownState(),
      }).catch((e: unknown) => {
        logger.error("Error sending known state", { err: e });
      });

      peer.toldKnownState.add(id);
    }
  }

  async startPeerReconciliation(peer: PeerState) {
    const coValuesOrderedByDependency: CoValueCore[] = [];

    const gathered = new Set<string>();

    const buildOrderedCoValueList = (coValue: CoValueCore) => {
      if (gathered.has(coValue.id)) {
        return;
      }

      gathered.add(coValue.id);

      for (const id of coValue.getDependedOnCoValues()) {
        const entry = this.local.coValuesStore.get(id);

        if (entry.isAvailable()) {
          buildOrderedCoValueList(entry.core);
        }
      }

      coValuesOrderedByDependency.push(coValue);
    };

    for (const entry of this.local.coValuesStore.getValues()) {
      if (!entry.isAvailable()) {
        // If the coValue is unavailable and we never tried this peer
        // we try to load it from the peer
        if (!peer.toldKnownState.has(entry.id)) {
          await entry.loadFromPeers([peer]).catch((e: unknown) => {
            logger.error("Error sending load", { err: e });
          });
        }
      } else {
        const coValue = entry.core;

        // Build the list of coValues ordered by dependency
        // so we can send the load message in the correct order
        buildOrderedCoValueList(coValue);
      }

      // Fill the missing known states with empty known states
      if (!peer.optimisticKnownStates.has(entry.id)) {
        peer.setOptimisticKnownState(entry.id, "empty");
      }
    }

    for (const coValue of coValuesOrderedByDependency) {
      /**
       * We send the load messages to:
       * - Subscribe to the coValue updates
       * - Start the sync process in case we or the other peer
       *   lacks some transactions
       */
      peer.toldKnownState.add(coValue.id);
      this.trySendToPeer(peer, {
        action: "load",
        ...coValue.knownState(),
      }).catch((e: unknown) => {
        logger.error("Error sending load", { err: e });
      });
    }
  }

  nextPeer: Map<PeerID, Peer> = new Map();

  async addPeer(peer: Peer) {
    const prevPeer = this.peers[peer.id];

    if (prevPeer) {
      // Assign to nextPeer to check against race conditions
      prevPeer.nextPeer = peer;

      if (!prevPeer.closed) {
        prevPeer.gracefulShutdown();
      }

      // Wait for the previous peer to finish processing the incoming messages
      await prevPeer.incomingMessagesProcessingPromise?.catch((e) => {});

      // If another peer was added in the meantime, we close this peer
      if (prevPeer.nextPeer !== peer) {
        peer.outgoing.close();
        return;
      }
    }

    const peerState = new PeerState(peer, prevPeer?.knownStates);
    this.peers[peer.id] = peerState;

    this.peersCounter.add(1, { role: peer.role });

    const unsubscribeFromKnownStatesUpdates = peerState.knownStates.subscribe(
      (id) => {
        this.syncState.triggerUpdate(peer.id, id);
      },
    );

    if (peerState.isServerOrStoragePeer()) {
      void this.startPeerReconciliation(peerState);
    }

    peerState
      .processIncomingMessages(async (msg) => {
        await this.handleSyncMessage(msg, peerState);
      })
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
        peerState.gracefulShutdown();
        unsubscribeFromKnownStatesUpdates();
        this.peersCounter.add(-1, { role: peer.role });

        if (peer.deletePeerStateOnClose && this.peers[peer.id] === peerState) {
          delete this.peers[peer.id];
        }
      });
  }

  trySendToPeer(peer: PeerState, msg: SyncMessage) {
    return peer.pushOutgoingMessage(msg);
  }

  /**
   * Handles the load message from a peer.
   *
   * Differences with the known state message:
   * - The load message triggers the CoValue loading process on the other peer
   * - The peer known state is stored as-is instead of being merged
   * - The load message always replies with a known state message
   */
  async handleLoad(msg: LoadMessage, peer: PeerState) {
    /**
     * We use the msg sessions as source of truth for the known states
     *
     * This way we can track part of the data loss that may occur when the other peer is restarted
     *
     */
    peer.setKnownState(msg.id, knownStateIn(msg));
    const entry = this.local.coValuesStore.get(msg.id);

    if (
      entry.highLevelState === "unknown" ||
      entry.highLevelState === "unavailable"
    ) {
      const eligiblePeers = this.getServerAndStoragePeers(peer.id);

      if (eligiblePeers.length === 0) {
        // We don't have any eligible peers to load the coValue from
        // so we send a known state back to the sender to let it know
        // that the coValue is unavailable
        peer.toldKnownState.add(msg.id);

        this.trySendToPeer(peer, {
          action: "known",
          id: msg.id,
          header: false,
          sessions: {},
        }).catch((e) => {
          logger.error("Error sending known state back", { err: e });
        });

        return;
      } else {
        // Should move the state to loading
        this.local.loadCoValueCore(msg.id, peer.id).catch((e) => {
          logger.error("Error loading coValue in handleLoad", { err: e });
        });
      }
    }

    if (entry.highLevelState === "loading") {
      // We need to return from handleLoad immediately and wait for the CoValue to be loaded
      // in a new task, otherwise we might block further incoming content messages that would
      // resolve the CoValue as available. This can happen when we receive fresh
      // content from a client, but we are a server with our own upstream server(s)
      entry
        .getCoValue()
        .then(async (value) => {
          if (value === "unavailable") {
            peer.toldKnownState.add(msg.id);

            this.trySendToPeer(peer, {
              action: "known",
              id: msg.id,
              header: false,
              sessions: {},
            }).catch((e) => {
              logger.error("Error sending known state back", { err: e });
            });

            return;
          }

          await this.sendNewContentIncludingDependencies(msg.id, peer);
        })
        .catch((e) => {
          logger.error("Error loading coValue in handleLoad loading state", {
            err: e,
          });
        });
    } else if (entry.isAvailable()) {
      await this.sendNewContentIncludingDependencies(msg.id, peer);
    } else {
      this.trySendToPeer(peer, {
        action: "known",
        id: msg.id,
        header: false,
        sessions: {},
      });
    }
  }

  async handleKnownState(msg: KnownStateMessage, peer: PeerState) {
    const entry = this.local.coValuesStore.get(msg.id);

    peer.combineWith(msg.id, knownStateIn(msg));

    // The header is a boolean value that tells us if the other peer do have information about the header.
    // If it's false in this point it means that the coValue is unavailable on the other peer.
    const availableOnPeer = peer.optimisticKnownStates.get(msg.id)?.header;

    if (!availableOnPeer) {
      entry.markNotFoundInPeer(peer.id);
    }

    if (entry.isAvailable()) {
      await this.sendNewContentIncludingDependencies(msg.id, peer);
    }
  }

  recordTransactionsSize(newTransactions: Transaction[], source: string) {
    for (const tx of newTransactions) {
      const txLength =
        tx.privacy === "private"
          ? tx.encryptedChanges.length
          : tx.changes.length;

      this.transactionsSizeHistogram.record(txLength, {
        source,
      });
    }
  }

  async handleNewContent(msg: NewContentMessage, peer: PeerState) {
    const entry = this.local.coValuesStore.get(msg.id);

    let coValue: CoValueCore;

    if (!entry.isAvailable()) {
      if (!msg.header) {
        this.trySendToPeer(peer, {
          action: "known",
          isCorrection: true,
          id: msg.id,
          header: false,
          sessions: {},
        }).catch((e) => {
          logger.error("Error sending known state correction", {
            peerId: peer.id,
            peerRole: peer.role,
            err: e,
          });
        });
        return;
      }

      peer.updateHeader(msg.id, true);

      coValue = new CoValueCore(msg.header, this.local);

      entry.markAvailable(coValue, peer.id);
    } else {
      coValue = entry.core;
    }

    let invalidStateAssumed = false;

    for (const [sessionID, newContentForSession] of Object.entries(msg.new) as [
      SessionID,
      SessionNewContent,
    ][]) {
      const ourKnownTxIdx =
        coValue.sessionLogs.get(sessionID)?.transactions.length;
      const theirFirstNewTxIdx = newContentForSession.after;

      if ((ourKnownTxIdx || 0) < theirFirstNewTxIdx) {
        invalidStateAssumed = true;
        continue;
      }

      const alreadyKnownOffset = ourKnownTxIdx
        ? ourKnownTxIdx - theirFirstNewTxIdx
        : 0;

      const newTransactions =
        newContentForSession.newTransactions.slice(alreadyKnownOffset);

      if (newTransactions.length === 0) {
        continue;
      }

      const result = coValue.tryAddTransactions(
        sessionID,
        newTransactions,
        undefined,
        newContentForSession.lastSignature,
      );

      if (result.isErr()) {
        logger.error("Failed to add transactions", {
          peerId: peer.id,
          peerRole: peer.role,
          id: msg.id,
          err: result.error,
        });
        entry.markErrored(peer.id, result.error);
        continue;
      }

      this.recordTransactionsSize(newTransactions, peer.role);

      peer.updateSessionCounter(
        msg.id,
        sessionID,
        newContentForSession.after +
          newContentForSession.newTransactions.length,
      );
    }

    if (invalidStateAssumed) {
      this.trySendToPeer(peer, {
        action: "known",
        isCorrection: true,
        ...coValue.knownState(),
      }).catch((e) => {
        logger.error("Error sending known state correction", {
          peerId: peer.id,
          peerRole: peer.role,
          err: e,
        });
      });
      peer.toldKnownState.add(msg.id);
    } else {
      /**
       * We are sending a known state message to the peer to acknowledge the
       * receipt of the new content.
       *
       * This way the sender knows that the content has been received and applied
       * and can update their peer's knownState accordingly.
       */
      this.trySendToPeer(peer, {
        action: "known",
        ...coValue.knownState(),
      }).catch((e: unknown) => {
        logger.error("Error sending known state", {
          peerId: peer.id,
          peerRole: peer.role,
          err: e,
        });
      });
      peer.toldKnownState.add(msg.id);
    }

    /**
     * We do send a correction/ack message before syncing to give an immediate
     * response to the peers that are waiting for confirmation that a coValue is
     * fully synced
     */
    this.syncCoValue(coValue);
  }

  async handleCorrection(msg: KnownStateMessage, peer: PeerState) {
    peer.setKnownState(msg.id, knownStateIn(msg));

    return this.sendNewContentIncludingDependencies(msg.id, peer);
  }

  handleUnsubscribe(_msg: DoneMessage) {
    throw new Error("Method not implemented.");
  }

  async syncCoValue(coValue: CoValueCore) {
    if (this.requestedSyncs[coValue.id]) {
      this.requestedSyncs[coValue.id]!.nRequestsThisTick++;
      return this.requestedSyncs[coValue.id]!.done;
    } else {
      const done = new Promise<void>((resolve) => {
        queueMicrotask(async () => {
          delete this.requestedSyncs[coValue.id];
          await this.actuallySyncCoValue(coValue);
          resolve();
        });
      });
      const entry = {
        done,
        nRequestsThisTick: 1,
      };
      this.requestedSyncs[coValue.id] = entry;
      return done;
    }
  }

  async actuallySyncCoValue(coValue: CoValueCore) {
    for (const peer of this.peersInPriorityOrder()) {
      if (peer.closed) continue;
      if (this.local.coValuesStore.get(coValue.id).isErroredInPeer(peer.id))
        continue;

      if (peer.optimisticKnownStates.has(coValue.id)) {
        await this.sendNewContentIncludingDependencies(coValue.id, peer);
      } else if (peer.isServerOrStoragePeer()) {
        await this.sendNewContentIncludingDependencies(coValue.id, peer);
      }
    }

    for (const peer of this.getPeers()) {
      this.syncState.triggerUpdate(peer.id, coValue.id);
    }
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
        coValue.highLevelState === "available" ||
        coValue.highLevelState === "loading",
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

function knownStateIn(msg: LoadMessage | KnownStateMessage) {
  return {
    id: msg.id,
    header: msg.header,
    sessions: msg.sessions,
  };
}
