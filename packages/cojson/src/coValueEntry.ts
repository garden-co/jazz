import { CoValueCore } from "./coValueCore.js";
import { HYBRID_MESSAGING_MODE } from "./globals.js";
import { RawCoID } from "./ids.js";
import { PeerEntry, PeerID } from "./peer/index.js";

export const CO_VALUE_LOADING_MAX_RETRIES = 5;
export const CO_VALUE_LOADING_TIMEOUT = 30_000;

export class CoValueUnknownState {
  type = "unknown" as const;
}

export class CoValueLoadingState {
  type = "loading" as const;
  private peers = new Map<
    PeerID,
    ReturnType<typeof createResolvablePromise<void>>
  >();
  private resolveResult: (value: CoValueCore | "unavailable") => void;

  result: Promise<CoValueCore | "unavailable">;

  constructor(peersIds: Iterable<PeerID>) {
    this.peers = new Map();

    for (const peerId of peersIds) {
      this.peers.set(peerId, createResolvablePromise<void>());
    }

    const { resolve, promise } = createResolvablePromise<
      CoValueCore | "unavailable"
    >();

    this.result = promise;
    this.resolveResult = resolve;
  }

  markAsUnavailable(peerId: PeerID) {
    const entry = this.peers.get(peerId);

    if (entry) {
      entry.resolve();
    }

    this.peers.delete(peerId);

    // If none of the peers have the coValue, we resolve to unavailable
    if (this.peers.size === 0) {
      this.resolve("unavailable");
    }
  }

  resolve(value: CoValueCore | "unavailable") {
    this.resolveResult(value);
    for (const entry of this.peers.values()) {
      entry.resolve();
    }
    this.peers.clear();
  }

  // Wait for a specific peer to have a known state
  waitForPeer(peerId: PeerID) {
    const entry = this.peers.get(peerId);

    if (!entry) {
      return Promise.resolve();
    }

    return entry.promise;
  }
}

export class CoValueAvailableState {
  type = "available" as const;

  constructor(public coValue: CoValueCore) {}
}

export class CoValueUnavailableState {
  type = "unavailable" as const;
}

type CoValueStateAction =
  | {
      type: "load-requested";
      peersIds: PeerID[];
    }
  | {
      type: "not-found-in-peer";
      peerId: PeerID;
    }
  | {
      type: "available";
      coValue: CoValueCore;
    };

type CoValueStateType =
  | CoValueUnknownState
  | CoValueLoadingState
  | CoValueAvailableState
  | CoValueUnavailableState;

class UploadState {
  protected peers = new Map<
    PeerID,
    ReturnType<typeof createResolvablePromise<void>> & { completed?: boolean }
  >();

  unawarePeers = new Set<PeerID>();

  constructor(private readonly coValueEntry: CoValueEntry) {
    this.peers = new Map();
  }

  getUnawarePeerIds(): PeerID[] {
    return Array.from(this.unawarePeers);
  }
  setPendingForPeer(peerId: PeerID) {
    if (!(this.coValueEntry.state.type === "available")) {
      console.error(
        "Trying to set pending upload for a coValue that is not available",
        this.coValueEntry.id,
      );

      return;
    }
    const peerUploadState = this.peers.get(peerId);
    if (!peerUploadState) {
      this.peers.set(peerId, createResolvablePromise<void>());
    }
  }

  setCompletedForPeer(peerId: PeerID) {
    const peerUploadState = this.peers.get(peerId);
    if (!peerUploadState) {
      if (!HYBRID_MESSAGING_MODE) {
        // When two messaging protocol are stacked, this could produce some excessive "ack" messages
        // out of peer's "known" messages which would lead to this error. Should be ignored in HYBRID_MESSAGING_MODE
        console.error(
          "Trying to set complete for a coValue that is not uploaded to",
          peerId,
          this.coValueEntry.id,
        );
      }

      return;
    }

    peerUploadState.resolve();
    peerUploadState.completed = true;
    this.unawarePeers.delete(peerId);
  }

  waitForPeer(peerId: PeerID) {
    const peerUploadState = this.peers.get(peerId);
    if (!peerUploadState) {
      console.error(
        "Trying to wait for no pending upload into peer",
        peerId,
        this.coValueEntry.id,
      );

      return;
    }

    return peerUploadState?.promise;
  }

  isCoValueFullyUploadedIntoPeer(peerId: PeerID) {
    const peerUploadState = this.peers.get(peerId);

    return !!peerUploadState?.completed;
  }

  copyFrom(otherUploadState: UploadState) {
    for (let [peerId] of otherUploadState.peers) {
      this.setPendingForPeer(peerId);
      if (otherUploadState.isCoValueFullyUploadedIntoPeer(peerId)) {
        this.setCompletedForPeer(peerId);
      }
    }
  }
}

// NOTE Renamed CoValueState into CoValueEntry
export class CoValueEntry {
  promise?: Promise<CoValueCore | "unavailable">;
  private resolve?: (value: CoValueCore | "unavailable") => void;
  public uploadState: UploadState;

  constructor(
    public id: RawCoID,
    public state: CoValueStateType,
  ) {
    this.uploadState = new UploadState(this);
  }

  static Unknown(id: RawCoID) {
    return new CoValueEntry(id, new CoValueUnknownState());
  }

  static Loading(id: RawCoID, peersIds: Iterable<PeerID>) {
    return new CoValueEntry(id, new CoValueLoadingState(peersIds));
  }

  static Available(coValue: CoValueCore) {
    return new CoValueEntry(coValue.id, new CoValueAvailableState(coValue));
  }

  static Unavailable(id: RawCoID) {
    return new CoValueEntry(id, new CoValueUnavailableState());
  }

  async getCoValue() {
    if (this.state.type === "available") {
      return this.state.coValue;
    }
    if (this.state.type === "unavailable") {
      return "unavailable";
    }

    // If we don't have a resolved state we return a new promise
    // that will be resolved when the state will move to available or unavailable
    if (!this.promise) {
      const { promise, resolve } = createResolvablePromise<
        CoValueCore | "unavailable"
      >();

      this.promise = promise;
      this.resolve = resolve;
    }

    return this.promise;
  }

  private moveToState(value: CoValueStateType) {
    this.state = value;

    if (!this.resolve) {
      return;
    }

    // If the state is available we resolve the promise
    // and clear it to handle the possible transition from unavailable to available
    if (value.type === "available") {
      this.resolve(value.coValue);
      this.clearPromise();
    } else if (value.type === "unavailable") {
      this.resolve("unavailable");
      this.clearPromise();
    }
  }

  private clearPromise() {
    this.promise = undefined;
    this.resolve = undefined;
  }

  async loadFromPeers(
    peers: PeerEntry[],
    loadCoValueCallback: (
      coValueEntry: CoValueEntry,
      peers: PeerEntry[],
    ) => Promise<void>,
  ) {
    const state = this.state;

    if (state.type !== "unknown" && state.type !== "unavailable") {
      return;
    }

    if (peers.length === 0) {
      return;
    }

    const doLoad = async (peersToLoadFrom: PeerEntry[]) => {
      // If we are in the loading state we move to a new loading state
      // to reset all the loading promises
      if (this.state.type === "loading" || this.state.type === "unknown") {
        this.moveToState(
          new CoValueLoadingState(peersToLoadFrom.map((p) => p.id)),
        );
      }

      // Assign the current state to a variable to not depend on the state changes
      // that may happen while we wait for loadCoValueCallback to complete
      const currentState = this.state;

      // If we entered successfully the loading state, we load the coValue from the peers
      //
      // We may not enter the loading state if the coValue has become available in between
      // of the retries
      if (currentState.type === "loading") {
        await loadCoValueCallback(this, peersToLoadFrom);

        const result = await currentState.result;
        return result !== "unavailable";
      }

      return currentState.type === "available";
    };

    await doLoad(peers);

    // Retry loading from peers that have the retry flag enabled
    const peersWithRetry = peers.filter((p) =>
      p.shouldRetryUnavailableCoValues(),
    );

    if (peersWithRetry.length > 0) {
      // We want to exit early if the coValue becomes available in between the retries
      await Promise.race([
        this.getCoValue(),
        runWithRetry(
          () => doLoad(peersWithRetry),
          CO_VALUE_LOADING_MAX_RETRIES,
        ),
      ]);
    }

    // If after the retries the coValue is still loading, we consider the load failed
    if (this.state.type === "loading") {
      this.moveToState(new CoValueUnavailableState());
    }
  }

  moveToLoadingState(peers: PeerEntry[]) {
    if (this.state.type !== "unknown") {
      console.error(
        "Cannot move to loading state from",
        this.state.type,
        this.id,
      );
    } else {
      this.moveToState(new CoValueLoadingState(peers.map((p) => p.id)));
    }
  }

  dispatch(action: CoValueStateAction) {
    const currentState = this.state;

    switch (action.type) {
      case "available":
        if (currentState.type === "loading") {
          currentState.resolve(action.coValue);
        }

        // It should be always possible to move to the available state
        this.moveToState(new CoValueAvailableState(action.coValue));

        break;
      case "not-found-in-peer":
        if (currentState.type === "loading") {
          currentState.markAsUnavailable(action.peerId);
        }

        this.uploadState.unawarePeers.add(action.peerId);
        break;
    }
  }

  markAsNotFoundInPeer(peerId: PeerID) {
    this.dispatch({
      type: "not-found-in-peer",
      peerId,
    });
  }
}

async function runWithRetry<T>(fn: () => Promise<T>, maxRetries: number) {
  let retries = 1;

  while (retries < maxRetries) {
    /**
     * With maxRetries of 5 we should wait:
     * 300ms
     * 900ms
     * 2700ms
     * 8100ms
     */
    await sleep(3 ** retries * 100);

    const result = await fn();

    if (result === true) {
      return;
    }

    retries++;
  }
}

function createResolvablePromise<T>() {
  let resolve!: (value: T) => void;

  const promise = new Promise<T>((res) => {
    resolve = res;
  });

  return { promise, resolve };
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}