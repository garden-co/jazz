import { PeerKnownStates, ReadonlyPeerKnownStates } from "./PeerKnownStates.js";
import {
  PriorityBasedMessageQueue,
  QueueEntry,
} from "./PriorityBasedMessageQueue.js";
import { TryAddTransactionsError } from "./coValueCore.js";
import { RawCoID, SessionID } from "./ids.js";
import { logger } from "./logger.js";
import { CO_VALUE_PRIORITY } from "./priority.js";
import { CoValueKnownState, Peer, SyncMessage } from "./sync.js";

export class PeerState {
  private queue: PriorityBasedMessageQueue;

  incomingMessagesProcessingPromise: Promise<void> | undefined;
  nextPeer: Peer | undefined;

  constructor(
    private peer: Peer,
    knownStates: ReadonlyPeerKnownStates | undefined,
  ) {
    /**
     * We set as default priority HIGH to handle all the messages without a
     * priority property as HIGH priority.
     *
     * This way we consider all the non-content messsages as HIGH priority.
     */
    this.queue = new PriorityBasedMessageQueue(CO_VALUE_PRIORITY.HIGH, {
      peerRole: peer.role,
    });

    this._knownStates = knownStates?.clone() ?? new PeerKnownStates();

    // We assume that exchanges with storage peers are always successful
    // hence we don't need to differentiate between knownStates and optimisticKnownStates
    if (peer.role === "storage") {
      this._optimisticKnownStates = "assumeInfallible";
    } else {
      this._optimisticKnownStates =
        knownStates?.clone() ?? new PeerKnownStates();
    }
  }

  /**
   * Here we to collect all the known states that a given peer has told us about.
   *
   * This can be used to safely track the sync state of a coValue in a given peer.
   */
  readonly _knownStates: PeerKnownStates;

  get knownStates(): ReadonlyPeerKnownStates {
    return this._knownStates;
  }

  /**
   * This one collects the known states "optimistically".
   * We use it to keep track of the content we have sent to a given peer.
   *
   * The main difference with knownState is that this is updated when the content is sent to the peer without
   * waiting for any acknowledgement from the peer.
   */
  readonly _optimisticKnownStates: PeerKnownStates | "assumeInfallible";

  get optimisticKnownStates(): ReadonlyPeerKnownStates {
    if (this._optimisticKnownStates === "assumeInfallible") {
      return this.knownStates;
    }

    return this._optimisticKnownStates;
  }

  readonly toldKnownState: Set<RawCoID> = new Set();

  updateHeader(id: RawCoID, header: boolean) {
    this._knownStates.updateHeader(id, header);

    if (this._optimisticKnownStates !== "assumeInfallible") {
      this._optimisticKnownStates.updateHeader(id, header);
    }
  }

  combineWith(id: RawCoID, value: CoValueKnownState) {
    this._knownStates.combineWith(id, value);

    if (this._optimisticKnownStates !== "assumeInfallible") {
      this._optimisticKnownStates.combineWith(id, value);
    }
  }

  combineOptimisticWith(id: RawCoID, value: CoValueKnownState) {
    if (this._optimisticKnownStates === "assumeInfallible") {
      this._knownStates.combineWith(id, value);
    } else {
      this._optimisticKnownStates.combineWith(id, value);
    }
  }

  updateSessionCounter(id: RawCoID, sessionId: SessionID, value: number) {
    this._knownStates.updateSessionCounter(id, sessionId, value);

    if (this._optimisticKnownStates !== "assumeInfallible") {
      this._optimisticKnownStates.updateSessionCounter(id, sessionId, value);
    }
  }

  setKnownState(id: RawCoID, knownState: CoValueKnownState | "empty") {
    this._knownStates.set(id, knownState);

    if (this._optimisticKnownStates !== "assumeInfallible") {
      this._optimisticKnownStates.set(id, knownState);
    }
  }

  setOptimisticKnownState(
    id: RawCoID,
    knownState: CoValueKnownState | "empty",
  ) {
    if (this._optimisticKnownStates === "assumeInfallible") {
      this._knownStates.set(id, knownState);
    } else {
      this._optimisticKnownStates.set(id, knownState);
    }
  }

  get id() {
    return this.peer.id;
  }

  get role() {
    return this.peer.role;
  }

  get priority() {
    return this.peer.priority;
  }

  get crashOnClose() {
    return this.peer.crashOnClose;
  }

  shouldRetryUnavailableCoValues() {
    return this.peer.role === "server";
  }

  isServerOrStoragePeer() {
    return this.peer.role === "server" || this.peer.role === "storage";
  }

  private processing = false;
  public closed = false;

  async processQueue() {
    if (this.processing) {
      return;
    }

    this.processing = true;

    let entry: QueueEntry | undefined;
    while ((entry = this.queue.pull())) {
      // Awaiting the push to send one message at a time
      // This way when the peer is "under pressure" we can enqueue all
      // the coming messages and organize them by priority
      await this.peer.outgoing
        .push(entry.msg)
        .then(entry.resolve)
        .catch(entry.reject);
    }

    this.processing = false;
  }

  pushOutgoingMessage(msg: SyncMessage) {
    if (this.closed) {
      return Promise.resolve();
    }

    const promise = this.queue.push(msg);

    void this.processQueue();

    return promise;
  }

  get incoming() {
    if (this.closed) {
      return (async function* () {
        yield "Disconnected" as const;
      })();
    }

    return this.peer.incoming;
  }

  private closeQueue() {
    let entry: QueueEntry | undefined;
    while ((entry = this.queue.pull())) {
      // Using resolve here to avoid unnecessary noise in the logs
      entry.resolve();
    }
  }

  closeListeners = new Set<() => void>();

  addCloseListener(listener: () => void) {
    if (this.closed) {
      listener();
      return () => {};
    }

    this.closeListeners.add(listener);

    return () => {
      this.closeListeners.delete(listener);
    };
  }

  emitClose() {
    for (const listener of this.closeListeners) {
      listener();
    }

    this.closeListeners.clear();
  }

  gracefulShutdown() {
    logger.debug("Gracefully closing", {
      peerId: this.id,
      peerRole: this.role,
    });
    this.closeQueue();
    this.peer.outgoing.close();
    this.closed = true;
    this.emitClose();
  }

  async processIncomingMessages(callback: (msg: SyncMessage) => Promise<void>) {
    if (this.closed) {
      throw new Error("Peer is closed");
    }

    if (this.incomingMessagesProcessingPromise) {
      throw new Error("Incoming messages processing already in progress");
    }

    const processIncomingMessages = async () => {
      for await (const msg of this.incoming) {
        if (msg === "Disconnected") {
          break;
        }
        if (msg === "PingTimeout") {
          logger.error("Ping timeout from peer", {
            peerId: this.id,
            peerRole: this.role,
          });
          break;
        }

        await callback(msg);
      }
    };

    this.incomingMessagesProcessingPromise = processIncomingMessages();

    return this.incomingMessagesProcessingPromise;
  }
}
