import { ValueType } from "@opentelemetry/api";
import { UpDownCounter, metrics } from "@opentelemetry/api";
import { PeerState } from "./PeerState.js";
import { CoValueCore } from "./coValueCore.js";
import { RawCoID } from "./ids.js";
import { logger } from "./logger.js";
import { StorageDriver } from "./storage.js";
import { PeerID } from "./sync.js";

export const CO_VALUE_LOADING_CONFIG = {
  MAX_RETRIES: 2,
  TIMEOUT: 30_000,
};

export class CoValueUnknownState {
  type = "unknown" as const;
}

export class CoValueLoadingFromStorageState {
  type = "loading-from-storage" as const;
  private storageDriver: StorageDriver;
  public id: RawCoID;

  constructor(storageAdapter: StorageDriver, id: RawCoID) {
    this.storageDriver = storageAdapter;
    this.id = id;
  }

  async loadFromStorage(): Promise<CoValueCore | null> {
    try {
      const core = await this.storageDriver.get(this.id);

      return core;
    } catch (err) {
      logger.error(`Failed to load coValue ${this.id} from storage`, {
        error: err instanceof Error ? err.message : "",
        stack: err instanceof Error ? (err.stack ?? null) : null,
      });
      return null;
    }
  }
}

export class CoValueLoadingFromPeersState {
  type = "loading-from-peers" as const;
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
  | CoValueLoadingFromStorageState
  | CoValueLoadingFromPeersState
  | CoValueAvailableState
  | CoValueUnavailableState;

export class CoValueState {
  promise?: Promise<CoValueCore | "unavailable">;
  private resolve?: (value: CoValueCore | "unavailable") => void;
  private counter: UpDownCounter;

  constructor(
    public id: RawCoID,
    public state: CoValueStateType,
  ) {
    this.counter = metrics
      .getMeter("cojson")
      .createUpDownCounter("jazz.covalues.loaded", {
        description: "The number of covalues in the system",
        unit: "covalue",
        valueType: ValueType.INT,
      });

    this.counter.add(1, {
      state: this.state.type,
    });
  }

  static Unknown(id: RawCoID) {
    return new CoValueState(id, new CoValueUnknownState());
  }

  static Available(coValue: CoValueCore) {
    return new CoValueState(coValue.id, new CoValueAvailableState(coValue));
  }

  static LoadingFromStorage(id: RawCoID, storageDriver: StorageDriver) {
    return new CoValueState(
      id,
      new CoValueLoadingFromStorageState(storageDriver, id),
    );
  }

  static LoadingFromPeers(id: RawCoID, peersIds: Iterable<PeerID>) {
    return new CoValueState(id, new CoValueLoadingFromPeersState(peersIds));
  }

  isLoading() {
    return (
      this.state.type === "loading-from-storage" ||
      this.state.type === "loading-from-peers"
    );
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
    this.counter.add(-1, {
      state: this.state.type,
    });
    this.state = value;

    this.counter.add(1, {
      state: this.state.type,
    });

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

  async loadCoValue(storageDriver: StorageDriver | null, peers: PeerState[]) {
    const state = this.state;

    if (
      state.type === "loading-from-storage" ||
      state.type === "loading-from-peers" ||
      state.type === "available"
    ) {
      return;
    }

    if (storageDriver) {
      const loadingState = new CoValueLoadingFromStorageState(
        storageDriver,
        this.id,
      );
      this.moveToState(loadingState);
      const coValue = await loadingState.loadFromStorage();
      if (coValue) {
        this.moveToState(new CoValueAvailableState(coValue));
        await loadCoValueFromPeers(this, getPeersWithoutErrors(peers, this.id));
        return;
      } else {
        this.moveToState(new CoValueUnknownState());
      }
    }

    if (peers.length === 0) {
      this.moveToState(new CoValueUnavailableState());
      return;
    }

    const doLoad = async (peersToLoadFrom: PeerState[]) => {
      const peersWithoutErrors = getPeersWithoutErrors(
        peersToLoadFrom,
        this.id,
      );

      // If we are in the loading state we move to a new loading state
      // to reset all the loading promises
      if (
        this.state.type === "loading-from-peers" ||
        this.state.type === "unknown" ||
        this.state.type === "unavailable"
      ) {
        this.moveToState(
          new CoValueLoadingFromPeersState(peersWithoutErrors.map((p) => p.id)),
        );
      }

      // Assign the current state to a variable to not depend on the state changes
      // that may happen while we wait for loadCoValueFromPeers to complete
      const currentState = this.state;

      // If we entered successfully the loading state, we load the coValue from the peers
      //
      // We may not enter the loading state if the coValue has become available in between
      // of the retries
      if (currentState.type === "loading-from-peers") {
        await loadCoValueFromPeers(this, peersWithoutErrors);

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
          CO_VALUE_LOADING_CONFIG.MAX_RETRIES,
        ),
      ]);
    }

    // If after the retries the coValue is still loading, we consider the load failed
    if (this.state.type === "loading-from-peers") {
      this.moveToState(new CoValueUnavailableState());
    }
  }

  dispatch(action: CoValueStateAction) {
    const currentState = this.state;

    switch (action.type) {
      case "available":
        if (currentState.type === "loading-from-peers") {
          currentState.resolve(action.coValue);
        }

        // It should be always possible to move to the available state
        this.moveToState(new CoValueAvailableState(action.coValue));

        break;
      case "not-found-in-peer":
        if (currentState.type === "loading-from-peers") {
          currentState.markAsUnavailable(action.peerId);
        }

        break;
    }
  }
}

async function loadCoValueFromPeers(
  coValueEntry: CoValueState,
  peers: PeerState[],
) {
  for (const peer of peers) {
    if (peer.closed) {
      continue;
    }

    if (coValueEntry.state.type === "available") {
      /**
       * We don't need to wait for the message to be delivered here.
       *
       * This way when the coValue becomes available because it's cached we don't wait for the server
       * peer to consume the messages queue before moving forward.
       */
      peer
        .pushOutgoingMessage({
          action: "load",
          ...coValueEntry.state.coValue.knownState(),
        })
        .catch((err) => {
          logger.warn(`Failed to push load message to peer ${peer.id}`, {
            err,
          });
        });
    } else {
      /**
       * We only wait for the load state to be resolved.
       */
      peer
        .pushOutgoingMessage({
          action: "load",
          id: coValueEntry.id,
          header: false,
          sessions: {},
        })
        .catch((err) => {
          logger.warn(`Failed to push load message to peer ${peer.id}`, {
            err,
          });
        });
    }

    if (coValueEntry.state.type === "loading-from-peers") {
      const { promise, resolve } = createResolvablePromise<void>();

      /**
       * Use a very long timeout for storage peers, because under pressure
       * they may take a long time to consume the messages queue
       *
       * TODO: Track errors on storage and do not rely on timeout
       */
      const timeoutDuration =
        peer.role === "storage"
          ? CO_VALUE_LOADING_CONFIG.TIMEOUT * 10
          : CO_VALUE_LOADING_CONFIG.TIMEOUT;

      const timeout = setTimeout(() => {
        if (coValueEntry.state.type === "loading-from-peers") {
          logger.warn("Failed to load coValue from peer", {
            coValueId: coValueEntry.id,
            peerId: peer.id,
            peerRole: peer.role,
          });
          coValueEntry.dispatch({
            type: "not-found-in-peer",
            peerId: peer.id,
          });
          resolve();
        }
      }, timeoutDuration);
      await Promise.race([promise, coValueEntry.state.waitForPeer(peer.id)]);
      clearTimeout(timeout);
    }
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

function getPeersWithoutErrors(peers: PeerState[], coValueId: RawCoID) {
  return peers.filter((p) => {
    if (p.erroredCoValues.has(coValueId)) {
      logger.warn(
        `Skipping load on errored coValue ${coValueId} from peer ${p.id}`,
      );
      return false;
    }

    return true;
  });
}
