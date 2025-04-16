import { RawCoID, SessionID } from "./ids.js";
import {
  CoValueKnownState,
  combinedKnownStates,
  emptyKnownState,
} from "./sync.js";

export class PeerKnownStates {
  private coValues = new Map<RawCoID, CoValueKnownState>();

  updateHeader(id: RawCoID, header: boolean) {
    const knownState = this.coValues.get(id) ?? emptyKnownState(id);
    knownState.header = header;
    this.coValues.set(id, knownState);
    this.triggerUpdate(id);
  }

  combineWith(id: RawCoID, value: CoValueKnownState) {
    const knownState = this.coValues.get(id) ?? emptyKnownState(id);
    this.coValues.set(id, combinedKnownStates(knownState, value));
    this.triggerUpdate(id);
  }

  updateSessionCounter(id: RawCoID, sessionId: SessionID, value: number) {
    const knownState = this.coValues.get(id) ?? emptyKnownState(id);
    const currentValue = knownState.sessions[sessionId] || 0;
    knownState.sessions[sessionId] = Math.max(currentValue, value);

    this.coValues.set(id, knownState);
    this.triggerUpdate(id);
  }

  set(id: RawCoID, knownState: CoValueKnownState | "empty") {
    this.coValues.set(
      id,
      knownState === "empty" ? emptyKnownState(id) : knownState,
    );
    this.triggerUpdate(id);
  }

  get(id: RawCoID) {
    return this.coValues.get(id);
  }

  has(id: RawCoID) {
    return this.coValues.has(id);
  }

  clone() {
    const clone = new PeerKnownStates();
    clone.coValues = new Map(this.coValues);
    return clone;
  }

  listeners = new Set<(id: RawCoID, knownState: CoValueKnownState) => void>();

  triggerUpdate(id: RawCoID) {
    this.trigger(id, this.coValues.get(id) ?? emptyKnownState(id));
  }

  private trigger(id: RawCoID, knownState: CoValueKnownState) {
    for (const listener of this.listeners) {
      listener(id, knownState);
    }
  }

  subscribe(listener: (id: RawCoID, knownState: CoValueKnownState) => void) {
    this.listeners.add(listener);

    return () => {
      this.listeners.delete(listener);
    };
  }
}

export type ReadonlyPeerKnownStates = Pick<
  PeerKnownStates,
  "get" | "has" | "clone" | "subscribe"
>;
