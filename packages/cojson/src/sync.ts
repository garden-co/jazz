import { md5 } from "@noble/hashes/legacy";
import { Histogram, ValueType, metrics } from "@opentelemetry/api";
import { PeerState } from "./PeerState.js";
import { SyncStateManager } from "./SyncStateManager.js";
import { UnsyncedCoValuesTracker } from "./UnsyncedCoValuesTracker.js";
import { SYNC_SCHEDULER_CONFIG } from "./config.js";
import {
  getContenDebugInfo,
  getNewTransactionsFromContentMessage,
  getSessionEntriesFromContentMessage,
  getTransactionSize,
  knownStateFromContent,
} from "./coValueContentMessage.js";
import { CoValueCore } from "./coValueCore/coValueCore.js";
import { CoValueHeader, Transaction } from "./coValueCore/verifiedState.js";
import { Signature } from "./crypto/crypto.js";
import { isDeleteSessionID, RawCoID, SessionID, isRawCoID } from "./ids.js";
import { LocalNode } from "./localNode.js";
import { logger } from "./logger.js";
import { CoValuePriority } from "./priority.js";
import { IncomingMessagesQueue } from "./queue/IncomingMessagesQueue.js";
import { LocalTransactionsSyncQueue } from "./queue/LocalTransactionsSyncQueue.js";
import type { StorageStreamingQueue } from "./queue/StorageStreamingQueue.js";
import {
  CoValueKnownState,
  knownStateFrom,
  KnownStateSessions,
} from "./knownState.js";
import { StorageAPI } from "./storage/index.js";

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

/**
 * Determines when network sync is enabled.
 * - "always": sync is enabled for both Anonymous Authentication and Authenticated Account
 * - "signedUp": sync is enabled when the user is authenticated
 * - "never": sync is disabled, content stays local
 * Can be dynamically modified to control sync behavior at runtime.
 */
export type SyncWhen = "always" | "signedUp" | "never";

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
    this.unsyncedTracker = new UnsyncedCoValuesTracker();

    this.transactionsSizeHistogram = metrics
      .getMeter("cojson")
      .createHistogram("jazz.transactions.size", {
        description: "The size of transactions in a covalue",
        unit: "bytes",
        valueType: ValueType.INT,
      });
  }

  syncState: SyncStateManager;
  unsyncedTracker: UnsyncedCoValuesTracker;

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

  getPersistentServerPeers(id: RawCoID): PeerState[] {
    return this.getServerPeers(id).filter((peer) => peer.persistent);
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

    const newContentPieces = coValue.newContentSince(
      peer.getOptimisticKnownState(id),
    );

    if (newContentPieces) {
      for (const piece of newContentPieces) {
        this.trySendToPeer(peer, piece);
      }

      peer.combineOptimisticWith(id, coValue.knownState());
    } else if (!peer.toldKnownState.has(id)) {
      if (coValue.isDeleted) {
        // This way we make the peer believe that we've always ingested all the content they sent, even though we skipped it because the coValue is deleted
        this.trySendToPeer(
          peer,
          coValue.stopSyncingKnownStateMessage(peer.getKnownState(id)),
        );
      } else {
        this.trySendToPeer(peer, {
          action: "known",
          ...coValue.knownStateWithStreaming(),
        });
      }
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

  async resumeUnsyncedCoValues(): Promise<void> {
    if (!this.local.storage) {
      // No storage available, skip resumption
      return;
    }

    await new Promise<void>((resolve, reject) => {
      // Load all persisted unsynced CoValues from storage
      this.local.storage?.getUnsyncedCoValueIDs((unsyncedCoValueIDs) => {
        const coValuesToLoad = unsyncedCoValueIDs.filter(
          (coValueId) => !this.local.hasCoValue(coValueId),
        );
        if (coValuesToLoad.length === 0) {
          resolve();
          return;
        }

        const BATCH_SIZE = 10;
        let processed = 0;

        const processBatch = async () => {
          const batch = coValuesToLoad.slice(processed, processed + BATCH_SIZE);

          await Promise.all(
            batch.map(
              async (coValueId) =>
                new Promise<void>((resolve) => {
                  try {
                    // Clear previous tracking (as it may include outdated peers)
                    this.local.storage?.stopTrackingSyncState(coValueId);

                    // Resume tracking sync state for this CoValue
                    // This will add it back to the tracker and set up subscriptions
                    this.trackSyncState(coValueId);

                    // Load the CoValue from storage (this will trigger sync if peers are connected)
                    const coValue = this.local.getCoValue(coValueId);
                    coValue.loadFromStorage((found) => {
                      if (!found) {
                        // CoValue could not be loaded from storage, stop tracking
                        this.unsyncedTracker.removeAll(coValueId);
                      }
                      resolve();
                    });
                  } catch (error) {
                    // Handle errors gracefully - log but don't fail the entire resumption
                    logger.warn(
                      `Failed to resume sync for CoValue ${coValueId}:`,
                      {
                        err: error,
                        coValueId,
                      },
                    );
                    this.unsyncedTracker.removeAll(coValueId);
                    resolve();
                  }
                }),
            ),
          );

          processed += batch.length;

          if (processed < coValuesToLoad.length) {
            processBatch().catch(reject);
          } else {
            resolve();
          }
        };

        processBatch().catch(reject);
      });
    });
  }

  startPeerReconciliation(peer: PeerState) {
    if (peer.role === "server" && peer.persistent) {
      // Resume syncing unsynced CoValues asynchronously
      this.resumeUnsyncedCoValues().catch((error) => {
        logger.warn("Failed to resume unsynced CoValues:", error);
      });
    }

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
      if (!peer.getKnownState(coValue.id)) {
        peer.setKnownState(coValue.id, "empty");
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

  messagesQueue = new IncomingMessagesQueue(() => this.processQueues());
  private processing = false;

  pushMessage(incoming: SyncMessage, peer: PeerState) {
    this.messagesQueue.push(incoming, peer);
  }

  /**
   * Get the storage streaming queue if available.
   * Returns undefined if storage doesn't have a streaming queue.
   */
  private getStorageStreamingQueue(): StorageStreamingQueue | undefined {
    const storage = this.local.storage;
    if (storage && "streamingQueue" in storage) {
      return storage.streamingQueue as StorageStreamingQueue;
    }
    return undefined;
  }

  /**
   * Unified queue processing that coordinates both incoming messages
   * and storage streaming entries.
   *
   * Processes items from both queues with priority ordering:
   * - Incoming messages are processed via round-robin across peers
   * - Storage streaming entries are processed by priority (MEDIUM before LOW)
   *
   * Implements time budget scheduling to avoid blocking the main thread.
   */
  private async processQueues() {
    if (this.processing) {
      return;
    }

    this.processing = true;
    let lastTimer = performance.now();

    const streamingQueue = this.getStorageStreamingQueue();

    while (true) {
      // First, try to pull from incoming messages queue
      const messageEntry = this.messagesQueue.pull();
      if (messageEntry) {
        try {
          this.handleSyncMessage(messageEntry.msg, messageEntry.peer);
        } catch (err) {
          logger.error("Error processing message", { err });
        }
      }

      // Then, try to pull from storage streaming queue
      const pushStreamingContent = streamingQueue?.pull();
      if (pushStreamingContent) {
        try {
          // Invoke the pushContent callback to stream the content
          pushStreamingContent();
        } catch (err) {
          logger.error("Error processing storage streaming entry", {
            err,
          });
        }
      }

      // If both queues are empty, we're done
      if (!messageEntry && !pushStreamingContent) {
        break;
      }

      // Check if we have blocked the main thread for too long
      // and if so, yield to the event loop
      const currentTimer = performance.now();
      if (
        currentTimer - lastTimer >
        SYNC_SCHEDULER_CONFIG.INCOMING_MESSAGES_TIME_BUDGET
      ) {
        await new Promise<void>((resolve) => setTimeout(resolve));
        lastTimer = performance.now();
      }
    }

    this.processing = false;
  }

  addPeer(peer: Peer, skipReconciliation: boolean = false) {
    const prevPeer = this.peers[peer.id];

    const peerState = prevPeer
      ? prevPeer.newPeerStateFrom(peer)
      : new PeerState(peer, undefined);

    this.peers[peer.id] = peerState;

    this.peersCounter.add(1, { role: peer.role });

    const unsubscribeFromKnownStatesUpdates =
      peerState.subscribeToKnownStatesUpdates((id, knownState) => {
        this.syncState.triggerUpdate(peer.id, id, knownState.value());
      });

    if (!skipReconciliation && peerState.role === "server") {
      this.startPeerReconciliation(peerState);
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
    const availableOnPeer = peer.getOptimisticKnownState(msg.id)?.header;

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

    // TODO: We can't handle client-to-client streaming until we
    // handle the streaming state reset on disconnection
    if (peer?.role === "client" && msg.expectContentUntil) {
      msg = {
        ...msg,
        expectContentUntil: undefined,
      };
    }

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
    } else if (msg.expectContentUntil) {
      coValue.verified.setStreamingKnownState(msg.expectContentUntil);
    }

    // At this point the CoValue must be in memory, if not we have a bug
    if (!coValue.hasVerifiedContent()) {
      throw new Error(
        "Unreachable: CoValue should always have a verified state at this point",
      );
    }

    let invalidStateAssumed = false;

    const validNewContent: NewContentMessage = {
      action: "content",
      id: msg.id,
      priority: msg.priority,
      header: msg.header,
      new: {},
    };

    let wasAlreadyDeleted = coValue.isDeleted;

    /**
     * The coValue is in memory, load the transactions from the content message
     */
    for (const [
      sessionID,
      newContentForSession,
    ] of getSessionEntriesFromContentMessage(msg)) {
      if (wasAlreadyDeleted && !isDeleteSessionID(sessionID)) {
        continue;
      }

      const newTransactions = getNewTransactionsFromContentMessage(
        newContentForSession,
        coValue.knownState(),
        sessionID,
      );

      if (newTransactions === undefined) {
        invalidStateAssumed = true;
        continue;
      }

      if (newTransactions.length === 0) {
        continue;
      }

      // TODO: Handle invalid signatures in the middle of streaming
      // This could cause a situation where we are unable to load a chunk, and ask for a correction for all the subsequent chunks
      const error = coValue.tryAddTransactions(
        sessionID,
        newTransactions,
        newContentForSession.lastSignature,
        this.skipVerify,
      );

      if (error) {
        if (peer) {
          logger.error("Failed to add transactions", {
            peerId: peer.id,
            peerRole: peer.role,
            id: msg.id,
            errorType: error.type,
            err: error.error,
            sessionID,
            msgKnownState: knownStateFromContent(msg).sessions,
            msgSummary: getContenDebugInfo(msg),
            knownState: coValue.knownState().sessions,
          });
          // TODO Mark only the session as errored, not the whole coValue
          coValue.markErrored(peer.id, error);
        } else {
          logger.error("Failed to add transactions from storage", {
            id: msg.id,
            err: error.error,
            sessionID,
            errorType: error.type,
          });
        }
        continue;
      }

      if (sourceRole && sourceRole !== "import") {
        this.recordTransactionsSize(newTransactions, sourceRole);
      }

      // We reset the new content for the deleted coValue
      // because we want to store only the delete session/transaction
      if (!wasAlreadyDeleted && coValue.isDeleted) {
        wasAlreadyDeleted = true;
        validNewContent.new = {};
      }

      // The new content for this session has been verified, so we can store it
      validNewContent.new[sessionID] = newContentForSession;
    }

    if (peer) {
      if (coValue.isDeleted) {
        // In case of deleted coValues, we combine the known state with the content message
        // to avoid that clients that don't support deleted coValues try to sync their own content indefinitely
        peer.combineWith(msg.id, knownStateFromContent(msg));
      } else {
        peer.combineWith(msg.id, knownStateFromContent(validNewContent));
      }
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
      if (coValue.isDeleted) {
        // This way we make the peer believe that we've ingested all the content, even though we skipped it because the coValue is deleted
        this.trySendToPeer(
          peer,
          coValue.stopSyncingKnownStateMessage(peer.getKnownState(msg.id)),
        );
      } else {
        this.trySendToPeer(peer, {
          action: "known",
          ...coValue.knownState(),
        });
      }
      peer.trackToldKnownState(msg.id);
    }

    const syncedPeers = [];

    /**
     * Store the content and propagate it to the server peers and the subscribed client peers
     */
    const hasNewContent =
      validNewContent.header || Object.keys(validNewContent.new).length > 0;

    if (from !== "storage" && hasNewContent) {
      this.storeContent(validNewContent);
      if (from === "import") {
        this.trackSyncState(coValue.id);
      }
    }

    for (const peer of this.getPeers(coValue.id)) {
      /**
       * We sync the content against the source peer if it is a client or server peers
       * to upload any content that is available on the current node and not on the source peer.
       */
      if (peer.closed || coValue.isErroredInPeer(peer.id)) {
        peer.emitCoValueChange(coValue.id);
        continue;
      }

      // We directly forward the new content to peers that have an active subscription
      if (peer.isCoValueSubscribedToPeer(coValue.id)) {
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
  }

  handleCorrection(msg: KnownStateMessage, peer: PeerState) {
    peer.setKnownState(msg.id, knownStateFrom(msg));

    return this.sendNewContent(msg.id, peer);
  }

  private syncQueue = new LocalTransactionsSyncQueue((content) =>
    this.syncContent(content),
  );
  syncLocalTransaction = this.syncQueue.syncTransaction;
  trackDirtyCoValues = this.syncQueue.trackDirtyCoValues;

  syncContent(content: NewContentMessage) {
    const coValue = this.local.getCoValue(content.id);

    this.storeContent(content);

    this.trackSyncState(coValue.id);

    const contentKnownState = knownStateFromContent(content);

    for (const peer of this.getPeers(coValue.id)) {
      // Only subscribed CoValues are synced to clients
      if (
        peer.role === "client" &&
        !peer.isCoValueSubscribedToPeer(coValue.id)
      ) {
        continue;
      }

      if (peer.closed || coValue.isErroredInPeer(peer.id)) {
        peer.emitCoValueChange(content.id);
        continue;
      }

      // We assume that the peer already knows anything before this content
      // Any eventual reconciliation will be handled through the known state messages exchange
      this.trySendToPeer(peer, content);
      peer.combineOptimisticWith(coValue.id, contentKnownState);
      peer.trackToldKnownState(coValue.id);
    }
  }

  private trackSyncState(coValueId: RawCoID): void {
    const peers = this.getPersistentServerPeers(coValueId);

    const isSyncRequired = this.local.syncWhen !== "never";
    if (isSyncRequired && peers.length === 0) {
      this.unsyncedTracker.add(coValueId);
      return;
    }

    for (const peer of peers) {
      if (this.syncState.isSynced(peer, coValueId)) {
        continue;
      }
      const alreadyTracked = this.unsyncedTracker.add(coValueId, peer.id);
      if (alreadyTracked) {
        continue;
      }

      const unsubscribe = this.syncState.subscribeToPeerUpdates(
        peer.id,
        coValueId,
        (_knownState, syncState) => {
          if (syncState.uploaded) {
            this.unsyncedTracker.remove(coValueId, peer.id);
            unsubscribe();
          }
        },
      );
    }
  }

  private storeContent(content: NewContentMessage) {
    const storage = this.local.storage;

    if (!storage) return;

    const value = this.local.getCoValue(content.id);

    if (value.isDeleted) {
      storage.markCoValueAsDeleted(value.id);
    }

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

      return value.newContentSince(correction);
    });
  }

  waitForSyncWithPeer(peerId: PeerID, id: RawCoID, timeout: number) {
    const peerState = this.peers[peerId];

    // The peer has been closed and is not persistent, so it isn't possible to sync
    if (!peerState) {
      return;
    }

    if (peerState.isCoValueSubscribedToPeer(id)) {
      const isAlreadySynced = this.syncState.isSynced(peerState, id);

      if (isAlreadySynced) {
        return;
      }
    } else if (peerState.role === "client") {
      // The client isn't subscribed to the coValue, so we won't sync it
      return;
    }

    return new Promise((resolve, reject) => {
      const unsubscribe = this.syncState.subscribeToPeerUpdates(
        peerId,
        id,
        (_knownState, syncState) => {
          if (syncState.uploaded) {
            resolve(true);
            unsubscribe?.();
            clearTimeout(timeoutId);
          }
        },
      );

      const timeoutId = setTimeout(() => {
        const coValue = this.local.getCoValue(id);
        const erroredInPeer = coValue.getErroredInPeerError(peerId);
        const knownState = coValue.knownState().sessions;
        const peerKnownState = peerState.getKnownState(id)?.sessions ?? {};
        let errorMessage = `Timeout on waiting for sync with peer ${peerId} for coValue ${id}:
  Known state: ${JSON.stringify(knownState)}
  Peer state: ${JSON.stringify(peerKnownState)}
`;

        if (erroredInPeer) {
          errorMessage += `\nMarked as errored: "${erroredInPeer}"`;
        }

        reject(new Error(errorMessage));
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

  setStorage(storage: StorageAPI) {
    this.unsyncedTracker.setStorage(storage);

    const storageStreamingQueue = this.getStorageStreamingQueue();
    if (storageStreamingQueue) {
      storageStreamingQueue.setListener(() => {
        this.processQueues();
      });
    }
  }

  removeStorage() {
    this.unsyncedTracker.removeStorage();
  }

  /**
   * Closes all the peer connections and ensures the list of unsynced coValues is persisted to storage.
   * @returns Promise of the current pending store operation, if any.
   */
  gracefulShutdown(): Promise<void> | undefined {
    for (const peer of Object.values(this.peers)) {
      peer.gracefulShutdown();
    }
    return this.unsyncedTracker.forcePersist();
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
