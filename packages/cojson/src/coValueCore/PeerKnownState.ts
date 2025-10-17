import type { RawCoID, SessionID } from "../ids.js";
import {
  cloneKnownState,
  combineKnownStates,
  CoValueKnownState,
  updateSessionCounter,
  emptyKnownState,
} from "../knownState.js";
import { PeerID } from "../sync.js";

export class PeerKnownState {
  readonly peerId: PeerID;
  private knownState: CoValueKnownState;
  private optimisticKnownState?: CoValueKnownState;

  constructor(id: RawCoID, peerId: PeerID) {
    this.peerId = peerId;
    this.knownState = emptyKnownState(id);
  }

  updateHeader(header: boolean) {
    this.knownState.header = header;

    if (this.optimisticKnownState) {
      this.optimisticKnownState.header = header;
    }
  }

  updateSessionCounter(sessionId: SessionID, value: number) {
    updateSessionCounter(this.knownState.sessions, sessionId, value);

    if (this.optimisticKnownState) {
      updateSessionCounter(
        this.optimisticKnownState.sessions,
        sessionId,
        value,
      );
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

  resetOptimisticState() {
    this.optimisticKnownState = undefined;
  }
}
