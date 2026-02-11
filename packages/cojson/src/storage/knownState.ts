import { type CoValueCore } from "../exports.js";
import { RawCoID } from "../ids.js";
import {
  CoValueKnownState,
  emptyKnownState,
  areCurrentSessionsInSyncWith,
} from "../knownState.js";

/**
 * Track how much data we have stored inside our storage
 * and provides the API to wait for the data to be fully stored.
 */
export class StorageKnownState {
  knownStates = new Map<string, CoValueKnownState>();

  getKnownState(id: string): CoValueKnownState {
    const knownState = this.knownStates.get(id);

    if (!knownState) {
      const empty = emptyKnownState(id as RawCoID);
      this.knownStates.set(id, empty);
      return empty;
    }

    return knownState;
  }

  /**
   * Get a cached knownState if it exists and has a header.
   * Unlike getKnownState, this doesn't create an empty state if one doesn't exist.
   */
  getCachedKnownState(id: string): CoValueKnownState | undefined {
    const knownState = this.knownStates.get(id);
    if (knownState?.header) {
      return knownState;
    }
    return undefined;
  }

  setKnownState(id: string, knownState: CoValueKnownState) {
    this.knownStates.set(id, knownState);
  }

  handleUpdate(id: string, knownState: CoValueKnownState) {
    const requests = this.waitForSyncRequests.get(id);

    if (!requests) {
      return;
    }

    for (const request of requests) {
      if (isInSync(request.knownState, knownState)) {
        request.resolve();
        requests.delete(request);
      }
    }
  }

  waitForSyncRequests = new Map<
    string,
    Set<{
      knownState: CoValueKnownState;
      resolve: (value: void) => void;
    }>
  >();

  waitForSync(id: string, coValue: CoValueCore) {
    const initialKnownState = coValue.knownState();
    if (isInSync(initialKnownState, this.getKnownState(id))) {
      return Promise.resolve();
    }

    const requests = this.waitForSyncRequests.get(id) || new Set();
    this.waitForSyncRequests.set(id, requests);

    return new Promise<void>((resolve) => {
      const unsubscribe = coValue.subscribe((coValue) => {
        req.knownState = coValue.knownState();
        this.handleUpdate(id, this.getKnownState(id));
      }, false);

      const handleResolve = () => {
        resolve();
        unsubscribe();
      };

      const req = { knownState: initialKnownState, resolve: handleResolve };

      requests.add(req);
    });
  }

  deleteKnownState(id: string) {
    this.knownStates.delete(id);

    for (const request of this.waitForSyncRequests.get(id) || []) {
      request.resolve();
    }
    this.waitForSyncRequests.delete(id);
  }

  clear() {
    this.knownStates.clear();
    this.waitForSyncRequests.clear();
  }
}

function isInSync(
  knownState: CoValueKnownState,
  knownStateFromStorage: CoValueKnownState,
) {
  if (!knownStateFromStorage.header && knownState.header) {
    return false;
  }

  return areCurrentSessionsInSyncWith(
    knownState.sessions,
    knownStateFromStorage.sessions,
  );
}
