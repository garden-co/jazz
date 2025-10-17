import { md5 } from "@noble/hashes/legacy";
import { Histogram, ValueType, metrics } from "@opentelemetry/api";
import { PeerState } from "./PeerState.js";
import { SyncStateManager } from "./SyncStateManager.js";
import {
  getContenDebugInfo,
  getTransactionSize,
  knownStateFromContent,
} from "./coValueContentMessage.js";
import { CoValueCore } from "./coValueCore/coValueCore.js";
import { CoValueHeader, Transaction } from "./coValueCore/verifiedState.js";
import { Signature } from "./crypto/crypto.js";
import { RawCoID, SessionID, isRawCoID } from "./ids.js";
import { LocalNode } from "./localNode.js";
import { logger } from "./logger.js";
import { CoValuePriority } from "./priority.js";
import { IncomingMessagesQueue } from "./queue/IncomingMessagesQueue.js";
import { LocalTransactionsSyncQueue } from "./queue/LocalTransactionsSyncQueue.js";
import {
  CoValueKnownState,
  knownStateFrom,
  KnownStateSessions,
} from "./knownState.js";

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
  expectContentUntil?: KnownStateSessions;
};

export type SessionNewContent = {
  // The index where to start appending the new transactions. The index counting starts from 1.
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

export interface IncomingPeerChannel {
  close: () => void;
  onMessage: (callback: (msg: SyncMessage | DisconnectedError) => void) => void;
  onClose: (callback: () => void) => void;
}

export interface OutgoingPeerChannel {
  push: (msg: SyncMessage | DisconnectedError) => void;
  close: () => void;
  onClose: (callback: () => void) => void;
}

export interface Peer {
  id: PeerID;
  incoming: IncomingPeerChannel;
  outgoing: OutgoingPeerChannel;
  role: "server" | "client";
  priority?: number;
  persistent?: boolean;
}

export type ServerPeerSelector = (
  id: RawCoID,
  serverPeers: PeerState[],
) => PeerState[];

export class SyncManager {
  peers: { [key: PeerID]: PeerState } = {};
  local: LocalNode;

  // When true, transactions will not be verified.
  // This is useful when syncing only for storage purposes, with the expectation that
  // the transactions have already been verified by the [trusted] peer that sent them.
  private skipVerify: boolean = false;

  // When true, coValues that arrive from server peers will be ignored if they had not
  // explicitly been requested via a load message.
  private _ignoreUnknownCoValuesFromServers: boolean = false;
  ignoreUnknownCoValuesFromServers() {
    this._ignoreUnknownCoValuesFromServers = true;
  }

  peersCounter = metrics.getMeter("cojson").createUpDownCounter("jazz.peers", {
    description: "Amount of connected peers",
    valueType: ValueType.INT,
    unit: "peer",
  });
  private transactionsSizeHistogram: Histogram;

  serverPeerSelector?: ServerPeerSelector;

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

  disableTransactionVerification() {
    this.skipVerify = true;
  }

  getPeers(id: RawCoID): PeerState[] {
    return this.getServerPeers(id).concat(this.getClientPeers());
  }

  getClientPeers(): PeerState[] {
    return Object.values(this.peers).filter((peer) => peer.role === "client");
  }

  getServerPeers(id: RawCoID, excludePeerId?: PeerID): PeerState[] {
    const serverPeers = Object.values(this.peers).filter(
      (peer) => peer.role === "server" && peer.id !== excludePeerId,
    );
    return this.serverPeerSelector
      ? this.serverPeerSelector(id, serverPeers)
      : serverPeers;
  }

  handleSyncMessage(msg: SyncMessage, peer: PeerState) {
    if (!isRawCoID(msg.id)) {
      const errorType = msg.id ? "invalid" : "undefined";
      logger.warn(`Received sync message with ${errorType} id`, {
        msg,
      });
      return;
    }

    // Prevent core shards from storing content that belongs to other shards.
    //
    // This can happen because a covalue "miss" on a core shard will cause a load message to
    // be sent to the original unsharded core. The original core, treating the peer as a client,
    // will respond with the covalue and its dependencies. Those dependencies might not belong
    // to this shard, so they should be ignored.
    //
    // TODO: remove once core has been sharded.
    if (
      peer.role === "server" &&
      this._ignoreUnknownCoValuesFromServers &&
      !this.local.hasCoValue(msg.id)
    ) {
      logger.warn(
        `Ignoring message ${msg.action} on unknown coValue ${msg.id} from peer ${peer.id}`,
      );
      return;
    }

    if (this.local.getCoValue(msg.id).isErroredInPeer(peer.id)) {
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

  sendNewContent(id: RawCoID, peer: PeerState, seen: Set<RawCoID> = new Set()) {
    if (seen.has(id)) {
      return;
    }

    seen.add(id);

    const coValue = this.local.getCoValue(id);

    if (!coValue.isAvailable()) {
      return;
    }

    const includeDependencies = peer.role !== "server";
    if (includeDependencies) {
      for (const dependency of coValue.getDependedOnCoValues()) {
        this.sendNewContent(dependency, peer, seen);
      }
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
        ...coValue.knownStateWithStreaming(),
      });
    }

    peer.trackToldKnownState(id);
  }

  reconcileServerPeers() {
    const serverPeers = Object.values(this.peers).filter(
      (peer) => peer.role === "server",
    );
    for (const peer of serverPeers) {
      this.startPeerReconciliation(peer);
    }
  }

  startPeerReconciliation(peer: PeerState) {
    const coValuesOrderedByDependency: CoValueCore[] = [];

    const seen = new Set<string>();
    const buildOrderedCoValueList = (coValue: CoValueCore) => {
      if (seen.has(coValue.id)) {
        return;
      }
      seen.add(coValue.id);

      // Ignore the covalue if this peer isn't relevant to it
      if (
        this.getServerPeers(coValue.id).find((p) => p.id === peer.id) ===
        undefined
      ) {
        return;
      }

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

  messagesQueue = new IncomingMessagesQueue();
  pushMessage(incoming: SyncMessage, peer: PeerState) {
    this.messagesQueue.push(incoming, peer);

    if (this.messagesQueue.processing) {
      return;
    }

    this.messagesQueue.processQueue((msg, peer) => {
      this.handleSyncMessage(msg, peer);
    });
  }

  addPeer(peer: Peer, skipReconciliation: boolean = false) {
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

    if (!skipReconciliation && peerState.role === "server") {
      void this.startPeerReconciliation(peerState);
    }

    peerState.incoming.onMessage((msg) => {
      if (msg === "Disconnected") {
        peerState.gracefulShutdown();
        return;
      }

      this.pushMessage(msg, peerState);
    });

    peerState.addCloseListener(() => {
      unsubscribeFromKnownStatesUpdates();
      this.peersCounter.add(-1, { role: peer.role });

      if (!peer.persistent && this.peers[peer.id] === peerState) {
        this.removePeer(peer.id);
      }
    });
  }

  removePeer(peerId: PeerID) {
    const peer = this.peers[peerId];
    if (!peer) {
      return;
    }
    if (!peer.closed) {
      peer.gracefulShutdown();
    }
    delete this.peers[peer.id];
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
    peer.setKnownState(msg.id, knownStateFrom(msg));
    const coValue = this.local.getCoValue(msg.id);

    if (coValue.isAvailable()) {
      this.sendNewContent(msg.id, peer);
      return;
    }

    const peers = this.getServerPeers(msg.id, peer.id);

    coValue.load(peers);

    const handleLoadResult = () => {
      if (coValue.isAvailable()) {
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

    peer.combineWith(msg.id, knownStateFrom(msg));

    // The header is a boolean value that tells us if the other peer do have information about the header.
    // If it's false in this point it means that the coValue is unavailable on the other peer.
    const availableOnPeer = peer.optimisticKnownStates.get(msg.id)?.header;

    if (!availableOnPeer) {
      coValue.markNotFoundInPeer(peer.id);
    }

    if (coValue.isAvailable()) {
      this.sendNewContent(msg.id, peer);
    }
  }

  recordTransactionsSize(newTransactions: Transaction[], source: string) {
    for (const tx of newTransactions) {
      const size = getTransactionSize(tx);

      this.transactionsSizeHistogram.record(size, {
        source,
      });
    }
  }

  handleNewContent(
    msg: NewContentMessage,
    from: PeerState | "storage" | "import",
  ) {
    const coValue = this.local.getCoValue(msg.id);
    const peer = from === "storage" || from === "import" ? undefined : from;
    const sourceRole =
      from === "storage"
        ? "storage"
        : from === "import"
          ? "import"
          : peer?.role;

    coValue.addDependenciesFromContentMessage(msg);

    // If some of the dependencies are missing, we wait for them to be available
    // before handling the new content
    // This must happen even if the dependencies are not related to this content
    // but the content we've got before
    if (!this.skipVerify && coValue.hasMissingDependencies()) {
      coValue.addNewContentToQueue(msg, from);

      for (const dependency of coValue.missingDependencies) {
        const dependencyCoValue = this.local.getCoValue(dependency);
        if (!dependencyCoValue.hasVerifiedContent()) {
          const peers = this.getServerPeers(dependency);

          // If the peer that sent the new content is a client, we can assume that they are in possession of the dependency
          if (peer?.role === "client") {
            peers.push(peer);
          }

          dependencyCoValue.load(peers);
        }
      }

      return;
    }

    /**
     * Check if we have the CoValue in memory
     */
    if (!coValue.hasVerifiedContent()) {
      /**
       * The peer has assumed we already have the CoValue
       */
      if (!msg.header) {
        // We check if the covalue was in memory and has been garbage collected
        // In that case we should have it tracked in the storage
        const storageKnownState = this.local.storage?.getKnownState(msg.id);

        if (storageKnownState?.header) {
          // If the CoValue has been garbage collected, we load it from the storage before handling the new content
          coValue.loadFromStorage((found) => {
            if (found) {
              this.handleNewContent(msg, from);
            } else {
              logger.error("Known CoValue not found in storage", {
                id: msg.id,
              });
            }
          });
          return;
        }

        // The peer assumption is not correct, so we ask for the full CoValue
        if (peer) {
          this.trySendToPeer(peer, {
            action: "known",
            isCorrection: true,
            id: msg.id,
            header: false,
            sessions: {},
          });
        } else {
          // The wrong assumption has been made by storage or import, we don't have a recovery mechanism
          // Should never happen
          logger.error(
            "Received new content with no header on a missing CoValue",
            {
              id: msg.id,
            },
          );
        }
        return;
      }

      const previousState = coValue.loadingState;

      /**
       * We are getting the full CoValue, so we can instantiate it
       */
      const success = coValue.provideHeader(
        msg.header,
        msg.expectContentUntil,
        this.skipVerify,
      );

      if (!success) {
        logger.error("Failed to provide header", {
          id: msg.id,
          header: msg.header,
        });
        return;
      }

      coValue.markFoundInPeer(peer?.id ?? "storage", previousState);
      peer?.updateHeader(msg.id, true);

      if (msg.expectContentUntil) {
        peer?.combineWith(msg.id, {
          id: msg.id,
          header: true,
          sessions: msg.expectContentUntil,
        });
      }
    }

    // At this point the CoValue must be in memory, if not we have a bug
    if (!coValue.hasVerifiedContent()) {
      throw new Error(
        "Unreachable: CoValue should always have a verified state at this point",
      );
    }

    let invalidStateAssumed = false;

    const contentToStore: NewContentMessage = {
      action: "content",
      id: msg.id,
      priority: msg.priority,
      header: msg.header,
      new: {},
    };

    /**
     * The coValue is in memory, load the transactions from the content message
     */
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

      // TODO: Handle invalid signatures in the middle of streaming
      // This could cause a situation where we are unable to load a chunk, and ask for a correction for all the subsequent chunks
      const result = coValue.tryAddTransactions(
        sessionID,
        newTransactions,
        newContentForSession.lastSignature,
        this.skipVerify,
      );

      if (result.isErr()) {
        if (peer) {
          logger.error("Failed to add transactions", {
            peerId: peer.id,
            peerRole: peer.role,
            id: msg.id,
            err: result.error,
          });
          // TODO Mark only the session as errored, not the whole coValue
          coValue.markErrored(peer.id, result.error);
        } else {
          logger.error("Failed to add transactions from storage", {
            id: msg.id,
            err: result.error,
          });
        }
        continue;
      }

      if (sourceRole && sourceRole !== "import") {
        this.recordTransactionsSize(newTransactions, sourceRole);
      }

      // The new content for this session has been verified, so we can store it
      if (result.value) {
        contentToStore.new[sessionID] = newContentForSession;
      }

      const transactionsCount =
        newContentForSession.after +
        newContentForSession.newTransactions.length;
      peer?.updateSessionCounter(msg.id, sessionID, transactionsCount);
    }

    /**
     * Check if we lack some transactions to be able to load the new content
     */
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
            content: getContenDebugInfo(msg),
            knownState: coValue.knownState(),
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

    const syncedPeers = [];

    /**
     * Store the content and propagate it to the server peers and the subscribed client peers
     */
    const hasNewContent =
      contentToStore.header || Object.keys(contentToStore.new).length > 0;

    if (from !== "storage" && hasNewContent) {
      this.storeContent(contentToStore);
    }

    for (const peer of this.getPeers(coValue.id)) {
      /**
       * We sync the content against the source peer if it is a client or server peers
       * to upload any content that is available on the current node and not on the source peer.
       */
      if (peer.closed) continue;
      if (coValue.isErroredInPeer(peer.id)) continue;

      // We directly forward the new content to peers that have an active subscription
      if (peer.optimisticKnownStates.has(coValue.id)) {
        this.sendNewContent(coValue.id, peer);
        syncedPeers.push(peer);
      } else if (
        peer.role === "server" &&
        !peer.loadRequestSent.has(coValue.id)
      ) {
        const state = coValue.getLoadingStateForPeer(peer.id);

        // Check if there is a inflight load operation and we
        // are waiting for other peers to send the load request
        if (state === "unknown") {
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

    /**
     * Send an update to all the sync state listeners
     */
    for (const peer of syncedPeers) {
      this.syncState.triggerUpdate(peer.id, coValue.id);
    }
  }

  handleCorrection(msg: KnownStateMessage, peer: PeerState) {
    peer.setKnownState(msg.id, knownStateFrom(msg));

    return this.sendNewContent(msg.id, peer);
  }

  private syncQueue = new LocalTransactionsSyncQueue((content) =>
    this.syncContent(content),
  );
  syncHeader = this.syncQueue.syncHeader;
  syncLocalTransaction = this.syncQueue.syncTransaction;
  trackDirtyCoValues = this.syncQueue.trackDirtyCoValues;

  syncContent(content: NewContentMessage) {
    const coValue = this.local.getCoValue(content.id);

    this.storeContent(content);

    const contentKnownState = knownStateFromContent(content);

    for (const peer of this.getPeers(coValue.id)) {
      if (peer.closed) continue;
      if (coValue.isErroredInPeer(peer.id)) continue;

      // Only subscribed CoValues are synced to clients
      if (
        peer.role === "client" &&
        !peer.optimisticKnownStates.has(coValue.id)
      ) {
        continue;
      }

      // We assume that the peer already knows anything before this content
      // Any eventual reconciliation will be handled through the known state messages exchange
      this.trySendToPeer(peer, content);
      peer.combineOptimisticWith(coValue.id, contentKnownState);
      peer.trackToldKnownState(coValue.id);
    }

    for (const peer of this.getPeers(coValue.id)) {
      this.syncState.triggerUpdate(peer.id, coValue.id);
    }
  }

  private storeContent(content: NewContentMessage) {
    const storage = this.local.storage;

    if (!storage) return;

    const value = this.local.getCoValue(content.id);

    // Try to store the content as-is for performance
    // In case that some transactions are missing, a correction will be requested, but it's an edge case
    storage.store(content, (correction) => {
      if (!value.verified) {
        logger.error(
          "Correction requested for a CoValue with no verified content",
          {
            id: content.id,
            content: getContenDebugInfo(content),
            correction,
            state: value.loadingState,
          },
        );
        return undefined;
      }

      return value.verified.newContentSince(correction);
    });
  }

  waitForSyncWithPeer(peerId: PeerID, id: RawCoID, timeout: number) {
    const { syncState } = this;
    const currentSyncState = syncState.getCurrentSyncState(peerId, id);

    const isTheConditionAlreadyMet = currentSyncState.uploaded;

    if (isTheConditionAlreadyMet) {
      return;
    }

    const peerState = this.peers[peerId];

    // The peer has been closed and is not persistent, so it isn't possible to sync
    if (!peerState) {
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

  waitForSync(id: RawCoID, timeout = 60_000) {
    const peers = this.getPeers(id);

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

/**
 * Returns a ServerPeerSelector that implements the Highest Weighted Random (HWR) algorithm.
 *
 * The HWR algorithm deterministically selects the top `n` peers for a given CoValue ID by assigning
 * each peer a "weight" based on the MD5 hash of the concatenation of the CoValue ID and the peer's ID.
 * The first 4 bytes of the hash are interpreted as a 32-bit unsigned integer, which serves as the peer's weight.
 * Peers are then sorted in descending order of weight, and the top `n` are selected.
 */
export function hwrServerPeerSelector(n: number): ServerPeerSelector {
  if (n === 0) {
    throw new Error("n must be greater than 0");
  }

  const enc = new TextEncoder();

  // Take the md5 hash of the peer ID and CoValue ID and convert the first 4 bytes to a 32-bit unsigned integer
  const getWeight = (id: RawCoID, peer: PeerState): number => {
    const hash = md5(enc.encode(id + peer.id));
    return (
      ((hash[0]! << 24) | (hash[1]! << 16) | (hash[2]! << 8) | hash[3]!) >>> 0
    );
  };

  return (id, serverPeers) => {
    if (serverPeers.length <= n) {
      return serverPeers;
    }

    const weightedPeers = serverPeers.map((peer) => {
      return {
        peer,
        weight: getWeight(id, peer),
      };
    });

    return weightedPeers
      .sort((a, b) => b.weight - a.weight)
      .slice(0, n)
      .map((wp) => wp.peer);
  };
}
