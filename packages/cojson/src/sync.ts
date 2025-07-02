import { Histogram, ValueType, metrics } from "@opentelemetry/api";
import { PeerState } from "./PeerState.js";
import { SyncStateManager } from "./SyncStateManager.js";
import { CoValueCore } from "./coValueCore/coValueCore.js";
import { getDependedOnCoValuesFromRawData } from "./coValueCore/utils.js";
import { CoValueHeader, Transaction } from "./coValueCore/verifiedState.js";
import { Signature } from "./crypto/crypto.js";
import { RawCoID, SessionID } from "./ids.js";
import { LocalNode } from "./localNode.js";
import { logger } from "./logger.js";
import { CoValuePriority } from "./priority.js";
import { accountOrAgentIDfromSessionID } from "./typeUtils/accountOrAgentIDfromSessionID.js";
import { isAccountID } from "./typeUtils/isAccountID.js";

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
  streamingTarget?: {
    [sessionID: SessionID]: number;
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
  role: "server" | "client";
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

  getServerPeers(excludePeerId?: PeerID): PeerState[] {
    return this.getPeers().filter(
      (peer) =>
        peer.role === "server" && peer.id !== excludePeerId && !peer.closed,
    );
  }

  handleSyncMessage(msg: SyncMessage, peer: PeerState) {
    if (msg.id === undefined || msg.id === null) {
      logger.warn("Received sync message with undefined id", {
        msg,
      });
      return;
    } else if (!msg.id.startsWith("co_z")) {
      logger.warn("Received sync message with invalid id", {
        msg,
      });
      return;
    } else if (this.local.getCoValue(msg.id).isErroredInPeer(peer.id)) {
      logger.warn(
        `Skipping message ${msg.action} on errored coValue ${msg.id} from peer ${peer.id}`,
      );
      return;
    }

    // TODO: validate
    switch (msg.action) {
      case "load":
        return this.handleLoad(msg, peer);
      case "known":
        if (msg.isCorrection) {
          return this.handleCorrection(msg, peer);
        } else {
          return this.handleKnownState(msg, peer);
        }
      case "content":
        return this.handleNewContent(msg, peer);
      case "done":
        return;
      default:
        throw new Error(
          `Unknown message type ${(msg as { action: "string" }).action}`,
        );
    }
  }

  sendNewContentIncludingDependencies(
    id: RawCoID,
    peer: PeerState,
    seen: Set<RawCoID> = new Set(),
  ) {
    if (seen.has(id)) {
      return;
    }

    seen.add(id);

    const coValue = this.local.getCoValue(id);

    if (!coValue.isAvailable()) {
      return;
    }

    for (const dependency of coValue.getDependedOnCoValues()) {
      this.sendNewContentIncludingDependencies(dependency, peer, seen);
    }

    const newContentPieces = coValue.verified.newContentSince(
      peer.optimisticKnownStates.get(id),
    );

    if (newContentPieces) {
      for (const piece of newContentPieces) {
        this.trySendToPeer(peer, piece);
      }

      peer.combineOptimisticWith(id, coValue.knownState());
    } else if (!peer.toldKnownState.has(id)) {
      this.trySendToPeer(peer, {
        action: "known",
        ...coValue.knownState(),
      });
    }

    peer.trackToldKnownState(id);
  }

  startPeerReconciliation(peer: PeerState) {
    const coValuesOrderedByDependency: CoValueCore[] = [];

    const gathered = new Set<string>();

    const buildOrderedCoValueList = (coValue: CoValueCore) => {
      if (gathered.has(coValue.id)) {
        return;
      }

      gathered.add(coValue.id);

      for (const id of coValue.getDependedOnCoValues()) {
        const coValue = this.local.getCoValue(id);

        if (coValue.isAvailable()) {
          buildOrderedCoValueList(coValue);
        }
      }

      coValuesOrderedByDependency.push(coValue);
    };

    for (const coValue of this.local.allCoValues()) {
      if (!coValue.isAvailable()) {
        // If the coValue is unavailable and we never tried this peer
        // we try to load it from the peer
        if (!peer.loadRequestSent.has(coValue.id)) {
          peer.trackLoadRequestSent(coValue.id);
          this.trySendToPeer(peer, {
            action: "load",
            header: false,
            id: coValue.id,
            sessions: {},
          });
        }
      } else {
        // Build the list of coValues ordered by dependency
        // so we can send the load message in the correct order
        buildOrderedCoValueList(coValue);
      }

      // Fill the missing known states with empty known states
      if (!peer.optimisticKnownStates.has(coValue.id)) {
        peer.setOptimisticKnownState(coValue.id, "empty");
      }
    }

    for (const coValue of coValuesOrderedByDependency) {
      /**
       * We send the load messages to:
       * - Subscribe to the coValue updates
       * - Start the sync process in case we or the other peer
       *   lacks some transactions
       */
      peer.trackLoadRequestSent(coValue.id);
      this.trySendToPeer(peer, {
        action: "load",
        ...coValue.knownState(),
      });
    }
  }

  addPeer(peer: Peer) {
    const prevPeer = this.peers[peer.id];

    if (prevPeer && !prevPeer.closed) {
      prevPeer.gracefulShutdown();
    }

    const peerState = new PeerState(peer, prevPeer?.knownStates);
    this.peers[peer.id] = peerState;

    this.peersCounter.add(1, { role: peer.role });

    const unsubscribeFromKnownStatesUpdates = peerState.knownStates.subscribe(
      (id) => {
        this.syncState.triggerUpdate(peer.id, id);
      },
    );

    if (peerState.role === "server") {
      void this.startPeerReconciliation(peerState);
    }

    peerState
      .processIncomingMessages((msg) => {
        this.handleSyncMessage(msg, peerState);
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
  handleLoad(msg: LoadMessage, peer: PeerState) {
    /**
     * We use the msg sessions as source of truth for the known states
     *
     * This way we can track part of the data loss that may occur when the other peer is restarted
     *
     */
    peer.setKnownState(msg.id, knownStateIn(msg));
    const coValue = this.local.getCoValue(msg.id);

    if (coValue.isAvailable()) {
      this.sendNewContentIncludingDependencies(msg.id, peer);
      return;
    }

    const peers = this.getServerPeers(peer.id);

    coValue.load(peers);

    const handleLoadResult = () => {
      if (coValue.isAvailable()) {
        this.sendNewContentIncludingDependencies(msg.id, peer);
        return;
      }

      peer.trackToldKnownState(msg.id);
      this.trySendToPeer(peer, {
        action: "known",
        id: msg.id,
        header: false,
        sessions: {},
      });
    };

    if (peers.length > 0 || this.local.storage) {
      coValue.waitForAvailableOrUnavailable().then(handleLoadResult);
    } else {
      handleLoadResult();
    }
  }

  handleKnownState(msg: KnownStateMessage, peer: PeerState) {
    const coValue = this.local.getCoValue(msg.id);

    peer.combineWith(msg.id, knownStateIn(msg));

    // The header is a boolean value that tells us if the other peer do have information about the header.
    // If it's false in this point it means that the coValue is unavailable on the other peer.
    const availableOnPeer = peer.optimisticKnownStates.get(msg.id)?.header;

    if (!availableOnPeer) {
      coValue.markNotFoundInPeer(peer.id);
    }

    if (coValue.isAvailable()) {
      this.sendNewContentIncludingDependencies(msg.id, peer);
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

  handleNewContent(msg: NewContentMessage, peer?: PeerState) {
    const coValue = this.local.getCoValue(msg.id);

    if (!coValue.hasVerifiedContent()) {
      if (!msg.header) {
        if (peer) {
          this.trySendToPeer(peer, {
            action: "known",
            isCorrection: true,
            id: msg.id,
            header: false,
            sessions: {},
          });
        } else {
          logger.error(
            "Received new content with no header on a missing CoValue",
            {
              id: msg.id,
            },
          );
        }
        return;
      }

      const sessionIDs = Object.keys(msg.new) as SessionID[];
      const transactions = Object.values(msg.new).map(
        (content) => content.newTransactions,
      );

      for (const dependency of getDependedOnCoValuesFromRawData(
        msg.id,
        msg.header,
        sessionIDs,
        transactions,
      )) {
        const dependencyCoValue = this.local.getCoValue(dependency);

        if (!dependencyCoValue.hasVerifiedContent()) {
          coValue.markMissingDependency(dependency);

          const peers = this.getServerPeers();

          // if the peer that sent the content is a client, we add it to the list of peers
          // to also ask them for the dependency
          if (peer?.role === "client") {
            peers.push(peer);
          }

          dependencyCoValue.load(peers);
        }
      }

      peer?.updateHeader(msg.id, true);
      coValue.provideHeader(
        msg.header,
        peer?.id ?? "storage",
        msg.streamingTarget,
      );

      if (msg.streamingTarget) {
        peer?.combineWith(msg.id, {
          id: msg.id,
          header: true,
          sessions: msg.streamingTarget,
        });
      }
    }

    if (!coValue.hasVerifiedContent()) {
      throw new Error(
        "Unreachable: CoValue should always have a verified state at this point",
      );
    }

    let invalidStateAssumed = false;

    for (const [sessionID, newContentForSession] of Object.entries(msg.new) as [
      SessionID,
      SessionNewContent,
    ][]) {
      const ourKnownTxIdx =
        coValue.verified.sessions.get(sessionID)?.transactions.length;
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

      const accountId = accountOrAgentIDfromSessionID(sessionID);

      if (isAccountID(accountId)) {
        const account = this.local.getCoValue(accountId);

        // We can't verify the transaction without the account, so we delay the session content handling until the account is available
        if (!account.isAvailable()) {
          // This covers the case where we are getting a new session on an already loaded coValue
          // where we need to load the account to get their public key
          if (!coValue.missingDependencies.has(accountId)) {
            const peers = this.getServerPeers();

            if (peer?.role === "client") {
              // if the peer that sent the content is a client, we add it to the list of peers
              // to also ask them for the dependency
              peers.push(peer);
            }

            account.load(peers);
          }

          // We need to wait for the account to be available before we can verify the transaction
          // Currently doing this by delaying the handleNewContent for the session to when we have the account
          //
          // This is not the best solution, because the knownState is not updated and the ACK response will be given
          // by excluding the session.
          // This is good enough implementation for now because the only case for the account to be missing are out-of-order
          // dependencies push, so the gap should be short lived.
          //
          // When we are going to have sharded-peers we should revisit this, and store unverified sessions that are considered as part of the
          // knwonState, but not actively used until they can be verified.
          void account.waitForAvailable().then(() => {
            this.handleNewContent(
              {
                action: "content",
                id: coValue.id,
                new: {
                  [sessionID]: newContentForSession,
                },
                priority: msg.priority,
              },
              peer,
            );
          });
          continue;
        }
      }

      const result = coValue.tryAddTransactions(
        sessionID,
        newTransactions,
        undefined,
        newContentForSession.lastSignature,
        "immediate", // TODO: can we change this to deferred?
      );

      if (result.isErr()) {
        if (peer) {
          logger.error("Failed to add transactions", {
            peerId: peer.id,
            peerRole: peer.role,
            id: msg.id,
            err: result.error,
          });
          coValue.markErrored(peer.id, result.error);
        } else {
          logger.error("Failed to add transactions from storage", {
            id: msg.id,
            err: result.error,
          });
        }
        continue;
      }

      this.recordTransactionsSize(newTransactions, peer?.role ?? "storage");

      peer?.updateSessionCounter(
        msg.id,
        sessionID,
        newContentForSession.after +
          newContentForSession.newTransactions.length,
      );
    }

    if (invalidStateAssumed) {
      if (peer) {
        this.trySendToPeer(peer, {
          action: "known",
          isCorrection: true,
          ...coValue.knownState(),
        });
        peer.trackToldKnownState(msg.id);
      } else {
        logger.error(
          "Invalid state assumed when handling new content from storage",
          {
            id: msg.id,
          },
        );
      }
    } else if (peer) {
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
      });
      peer.trackToldKnownState(msg.id);
    }

    const sourcePeer = peer;
    const isContentFromStorage = !sourcePeer;
    const syncedPeers = [];

    if (!isContentFromStorage) {
      this.storeCoValue(coValue, [msg]);
    }

    for (const peer of this.peersInPriorityOrder()) {
      /**
       * We sync the content against the source peer if it is a client or server peers
       * to upload any content that is available on the current node and not on the source peer.
       */
      if (peer.closed) continue;
      if (coValue.isErroredInPeer(peer.id)) continue;

      // We directly forward the new content to peers that have an active subscription
      if (peer.optimisticKnownStates.has(coValue.id)) {
        this.sendNewContentIncludingDependencies(coValue.id, peer);
        syncedPeers.push(peer);
      } else if (
        peer.role === "server" &&
        !peer.loadRequestSent.has(coValue.id)
      ) {
        const state = coValue.getStateForPeer(peer.id)?.type;

        // Check if there is a inflight load operation and we
        // are waiting for other peers to send the load request
        if (state === "unknown" || state === undefined) {
          // Sending a load message to the peer to get to know how much content is missing
          // before sending the new content
          this.trySendToPeer(peer, {
            action: "load",
            ...coValue.knownStateWithStreaming(),
          });
          peer.trackLoadRequestSent(coValue.id);
          syncedPeers.push(peer);
        }
      }
    }

    for (const peer of syncedPeers) {
      this.syncState.triggerUpdate(peer.id, coValue.id);
    }
  }

  handleCorrection(msg: KnownStateMessage, peer: PeerState) {
    peer.setKnownState(msg.id, knownStateIn(msg));

    return this.sendNewContentIncludingDependencies(msg.id, peer);
  }

  requestedSyncs = new Set<RawCoID>();
  requestCoValueSync(coValue: CoValueCore) {
    if (this.requestedSyncs.has(coValue.id)) {
      return;
    }

    queueMicrotask(() => {
      if (this.requestedSyncs.has(coValue.id)) {
        this.syncCoValue(coValue);
      }
    });

    this.requestedSyncs.add(coValue.id);
  }

  storeCoValue(coValue: CoValueCore, data: NewContentMessage[] | undefined) {
    const storage = this.local.storage;

    if (!storage || !data) return;

    // Try to store the content as-is for performance
    // In case that some transactions are missing, a correction will be requested, but it's an edge case
    storage.store(data, (correction) => {
      if (!coValue.hasVerifiedContent()) return;

      const newContentPieces = coValue.verified.newContentSince(correction);

      if (!newContentPieces) return;

      storage.store(newContentPieces, (response) => {
        logger.error(
          "Correction requested by storage after sending a correction content",
          {
            response,
            knownState: coValue.knownState(),
          },
        );
      });
    });
  }

  syncCoValue(coValue: CoValueCore) {
    this.requestedSyncs.delete(coValue.id);

    if (this.local.storage && coValue.hasVerifiedContent()) {
      const knownState = this.local.storage.getKnownState(coValue.id);
      const newContentPieces = coValue.verified.newContentSince(knownState);

      this.storeCoValue(coValue, newContentPieces);
    }

    for (const peer of this.peersInPriorityOrder()) {
      if (peer.closed) continue;
      if (coValue.isErroredInPeer(peer.id)) continue;

      // Only subscribed CoValues are synced to clients
      if (
        peer.role === "client" &&
        !peer.optimisticKnownStates.has(coValue.id)
      ) {
        continue;
      }

      this.sendNewContentIncludingDependencies(coValue.id, peer);
    }

    for (const peer of this.getPeers()) {
      this.syncState.triggerUpdate(peer.id, coValue.id);
    }
  }

  waitForSyncWithPeer(peerId: PeerID, id: RawCoID, timeout: number) {
    const { syncState } = this;
    const currentSyncState = syncState.getCurrentSyncState(peerId, id);

    const isTheConditionAlreadyMet = currentSyncState.uploaded;

    if (isTheConditionAlreadyMet) {
      return;
    }

    const peerState = this.peers[peerId];

    // The peer has been closed, so it isn't possible to sync
    if (!peerState || peerState.closed) {
      return;
    }

    // The client isn't subscribed to the coValue, so we won't sync it
    if (
      peerState.role === "client" &&
      !peerState.optimisticKnownStates.has(id)
    ) {
      return;
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

  waitForStorageSync(id: RawCoID) {
    return this.local.storage?.waitForSync(id, this.local.getCoValue(id));
  }

  waitForSync(id: RawCoID, timeout = 30_000) {
    const peers = this.getPeers();

    return Promise.all(
      peers
        .map((peer) => this.waitForSyncWithPeer(peer.id, id, timeout))
        .concat(this.waitForStorageSync(id)),
    );
  }

  waitForAllCoValuesSync(timeout = 60_000) {
    const coValues = this.local.allCoValues();
    const validCoValues = Array.from(coValues).filter(
      (coValue) =>
        coValue.loadingState === "available" ||
        coValue.loadingState === "loading",
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
