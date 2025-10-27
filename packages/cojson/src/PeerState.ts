import { PeerKnownState } from "./coValueCore/PeerKnownState.js";
import { RawCoID, SessionID } from "./ids.js";
import { CoValueKnownState } from "./knownState.js";
import { logger } from "./logger.js";
import { Peer, SyncMessage } from "./sync.js";

export class PeerState {
  /**
   * Here we to collect all the known states that a given peer has told us about.
   *
   * This can be used to safely track the sync state of a coValue in a given peer.
   */
  private readonly _knownStates: Map<RawCoID, PeerKnownState>;

  constructor(
    private peer: Peer,
    knownStates: Map<RawCoID, PeerKnownState> | undefined,
  ) {
    this._knownStates = knownStates ?? new Map();
  }

  getKnownState(id: RawCoID) {
    return this._knownStates.get(id)?.value();
  }

  getOptimisticKnownState(id: RawCoID) {
    return this._knownStates.get(id)?.optimisticValue();
  }

  isCoValueSubscribedToPeer(id: RawCoID) {
    return this._knownStates.has(id);
  }

  /**
   * Closes the current peer state and creates a new one from a given peer,
   * keeping the same known states.
   *
   * This is used to create a new peer state when a peer reconnects.
   */
  newPeerStateFrom(peer: Peer) {
    if (!this.closed) {
      this.gracefulShutdown();
    }

    const knownStates = new Map<RawCoID, PeerKnownState>();
    // On reconnect, we reset all the optimistic known states
    // because we can't know if those syncs were successful or not
    for (const knownState of this._knownStates.values()) {
      knownStates.set(knownState.id, knownState.cloneWithoutOptimistic());
    }

    return new PeerState(peer, knownStates);
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

  private getOrCreateKnownState(id: RawCoID) {
    let knownState = this._knownStates.get(id);

    if (!knownState) {
      knownState = new PeerKnownState(id, this.peer.id);
      this._knownStates.set(id, knownState);
    }

    return knownState;
  }

  updateHeader(id: RawCoID, header: boolean) {
    const knownState = this.getOrCreateKnownState(id);
    knownState.updateHeader(header);
    this.triggerUpdate(id, knownState);
  }

  combineWith(id: RawCoID, value: CoValueKnownState) {
    const knownState = this.getOrCreateKnownState(id);
    knownState.combineWith(value);
    this.triggerUpdate(id, knownState);
  }

  combineOptimisticWith(id: RawCoID, value: CoValueKnownState) {
    const knownState = this.getOrCreateKnownState(id);
    knownState.combineOptimisticWith(value);
    this.triggerUpdate(id, knownState);
  }

  setKnownState(id: RawCoID, payload: CoValueKnownState | "empty") {
    const knownState = this.getOrCreateKnownState(id);
    knownState.set(payload);
    this.triggerUpdate(id, knownState);
  }

  /**
   * Emit a change event for a given coValue.
   *
   * This is used to notify subscribers that the known state of a coValue has changed,
   * but the known state of the peer has not.
   */
  emitCoValueChange(id: RawCoID) {
    if (this.peer.role === "client" && !this.isCoValueSubscribedToPeer(id)) {
      return;
    }

    const knownState = this.getOrCreateKnownState(id);
    this.triggerUpdate(id, knownState);
  }

  listeners = new Set<(id: RawCoID, value: PeerKnownState) => void>();

  private triggerUpdate(id: RawCoID, value: PeerKnownState) {
    for (const listener of this.listeners) {
      listener(id, value);
    }
  }

  subscribeToKnownStatesUpdates(
    listener: (id: RawCoID, value: PeerKnownState) => void,
  ) {
    this.listeners.add(listener);

    return () => {
      this.listeners.delete(listener);
    };
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
