import { RawCoID, SessionID } from "./ids.js";
import { logger } from "./logger.js";
import { PeerKnownStates, ReadonlyPeerKnownStates } from "./PeerKnownStates.js";
import { CoValueKnownState, Peer, SyncMessage } from "./sync.js";

export class PeerState {
  constructor(
    private peer: Peer,
    knownStates: ReadonlyPeerKnownStates | undefined,
  ) {
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

  public closed = false;

  get incoming() {
    return this.peer.incoming;
  }

  get persistent() {
    return this.peer.persistent;
  }

  pushOutgoingMessage(msg: SyncMessage) {
    this.peer.outgoing.push(msg);
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
    if (this.closed) {
      return;
    }

    logger.debug("Gracefully closing", {
      peerId: this.id,
      peerRole: this.role,
    });

    this.closed = true;
    this.peer.outgoing.push("Disconnected");
    this.peer.outgoing.close();
    this.peer.incoming.close();
    this.emitClose();
  }
}
