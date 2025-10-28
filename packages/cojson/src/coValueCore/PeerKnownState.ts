import type { RawCoID } from "../ids.js";
import {
  cloneKnownState,
  combineKnownStates,
  CoValueKnownState,
  emptyKnownState,
} from "../knownState.js";
import { PeerID } from "../sync.js";

export class PeerKnownState {
  readonly id: RawCoID;
  readonly peerId: PeerID;
  private knownState: CoValueKnownState;
  private optimisticKnownState?: CoValueKnownState;

  constructor(id: RawCoID, peerId: PeerID) {
    this.id = id;
    this.peerId = peerId;
    this.knownState = emptyKnownState(id);
  }

  cloneWithoutOptimistic() {
    const clone = new PeerKnownState(this.id, this.peerId);
    clone.set(this.knownState);
    return clone;
  }

  updateHeader(header: boolean) {
    this.knownState.header = header;

    if (this.optimisticKnownState) {
      this.optimisticKnownState.header = header;
    }
  }

  combineWith(value: CoValueKnownState) {
    combineKnownStates(this.knownState, value);

    if (this.optimisticKnownState) {
      combineKnownStates(this.optimisticKnownState, value);
    }
  }

  combineOptimisticWith(value: CoValueKnownState) {
    if (!this.optimisticKnownState) {
      this.optimisticKnownState = cloneKnownState(this.knownState);
    }

    combineKnownStates(this.optimisticKnownState, value);
  }

  /**
   * Aligns the CoValue known state with the defined payload
   */
  set(payload: CoValueKnownState | "empty") {
    if (payload === "empty") {
      this.knownState.header = false;
      this.knownState.sessions = {};
    } else {
      this.knownState.header = payload.header;
      this.knownState.sessions = { ...payload.sessions };
    }

    this.optimisticKnownState = undefined;
  }

  value() {
    return this.knownState;
  }

  optimisticValue() {
    return this.optimisticKnownState ?? this.knownState;
  }
}
