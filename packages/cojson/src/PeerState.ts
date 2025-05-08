import { PeerKnownStates, ReadonlyPeerKnownStates } from "./PeerKnownStates.js";
import { PriorityBasedMessageQueue } from "./PriorityBasedMessageQueue.js";
import { RawCoID, SessionID } from "./ids.js";
import { logger } from "./logger.js";
import { CO_VALUE_PRIORITY } from "./priority.js";
import { CoValueKnownState, Peer, SyncMessage } from "./sync.js";

export class PeerState {
  private queue: PriorityBasedMessageQueue;

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
    this._optimisticKnownStates = knownStates?.clone() ?? new PeerKnownStates();
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
  readonly _optimisticKnownStates: PeerKnownStates;

  get optimisticKnownStates(): ReadonlyPeerKnownStates {
    return this._optimisticKnownStates;
  }

  readonly toldKnownState: Set<RawCoID> = new Set();
  readonly loadRequestSent: Set<RawCoID> = new Set();

  trackLoadRequestSent(id: RawCoID) {
    this.toldKnownState.add(id);
    this.loadRequestSent.add(id);
  }

  trackToldKnownState(id: RawCoID) {
    this.toldKnownState.add(id);
  }

  updateHeader(id: RawCoID, header: boolean) {
    this._knownStates.updateHeader(id, header);
    this._optimisticKnownStates.updateHeader(id, header);
  }

  combineWith(id: RawCoID, value: CoValueKnownState) {
    this._knownStates.combineWith(id, value);
    this._optimisticKnownStates.combineWith(id, value);
  }

  combineOptimisticWith(id: RawCoID, value: CoValueKnownState) {
    this._optimisticKnownStates.combineWith(id, value);
  }

  updateSessionCounter(id: RawCoID, sessionId: SessionID, value: number) {
    this._knownStates.updateSessionCounter(id, sessionId, value);
    this._optimisticKnownStates.updateSessionCounter(id, sessionId, value);
  }

  setKnownState(id: RawCoID, knownState: CoValueKnownState | "empty") {
    this._knownStates.set(id, knownState);
    this._optimisticKnownStates.set(id, knownState);
  }

  setOptimisticKnownState(
    id: RawCoID,
    knownState: CoValueKnownState | "empty",
  ) {
    this._optimisticKnownStates.set(id, knownState);
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

    let msg: SyncMessage | undefined;
    while ((msg = this.queue.pull())) {
      if (this.closed) {
        break;
      }

      // Awaiting the push to send one message at a time
      // This way when the peer is "under pressure" we can enqueue all
      // the coming messages and organize them by priority
      try {
        await this.peer.outgoing.push(msg);
      } catch (e) {
        logger.error("Error sending message", {
          err: e,
          action: msg.action,
          id: msg.id,
          peerId: this.id,
          peerRole: this.role,
        });
      }
    }

    this.processing = false;
  }

  pushOutgoingMessage(msg: SyncMessage) {
    if (this.closed) {
      return;
    }

    this.queue.push(msg);

    void this.processQueue();
  }

  isProcessing() {
    return this.processing;
  }

  get incoming() {
    if (this.closed) {
      return (async function* () {
        yield "Disconnected" as const;
      })();
    }

    return this.peer.incoming;
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
    this.peer.crashOnClose = false;
    this.peer.outgoing.close();
    this.closed = true;
    this.emitClose();
  }

  async processIncomingMessages(callback: (msg: SyncMessage) => void) {
    if (this.closed) {
      throw new Error("Peer is closed");
    }

    const processIncomingMessages = async () => {
      for await (const msg of this.incoming) {
        if (this.closed) {
          return;
        }

        if (msg === "Disconnected") {
          return;
        }

        if (msg === "PingTimeout") {
          logger.error("Ping timeout from peer", {
            peerId: this.id,
            peerRole: this.role,
          });
          return;
        }

        callback(msg);
      }
    };

    return processIncomingMessages();
  }
}
