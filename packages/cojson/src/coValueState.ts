import { ValueType } from "@opentelemetry/api";
import { UpDownCounter, metrics } from "@opentelemetry/api";
import { PeerState } from "./PeerState.js";
import { CoValueCore, TryAddTransactionsError } from "./coValueCore.js";
import { RawCoID } from "./ids.js";
import { logger } from "./logger.js";
import { PeerID, emptyKnownState } from "./sync.js";

export const CO_VALUE_LOADING_CONFIG = {
  MAX_RETRIES: 2,
  TIMEOUT: 30_000,
};

export class CoValueState {
  private peers = new Map<
    PeerID,
    | { type: "unknown" | "pending" | "available" | "unavailable" }
    | {
        type: "errored";
        error: TryAddTransactionsError;
      }
  >();

  core: CoValueCore | null = null;
  id: RawCoID;

  private listeners: Set<(state: CoValueState) => void> = new Set();
  private counter: UpDownCounter;

  constructor(id: RawCoID) {
    this.id = id;

    this.counter = metrics
      .getMeter("cojson")
      .createUpDownCounter("jazz.covalues.loaded", {
        description: "The number of covalues in the system",
        unit: "covalue",
        valueType: ValueType.INT,
      });

    this.updateCounter(null);
  }

  get highLevelState() {
    if (this.core) {
      return "available";
    } else if (this.peers.size === 0) {
      return "unknown";
    }

    for (const peer of this.peers.values()) {
      if (peer.type === "pending") {
        return "loading";
      } else if (peer.type === "unknown") {
        return "unknown";
      }
    }

    return "unavailable";
  }

  isErroredInPeer(peerId: PeerID) {
    return this.peers.get(peerId)?.type === "errored";
  }

  isAvailable(): this is { type: "available"; core: CoValueCore } {
    return !!this.core;
  }

  addListener(listener: (state: CoValueState) => void) {
    this.listeners.add(listener);
    listener(this);
  }

  removeListener(listener: (state: CoValueState) => void) {
    this.listeners.delete(listener);
  }

  private notifyListeners() {
    for (const listener of this.listeners) {
      listener(this);
    }
  }

  async getCoValue() {
    if (this.highLevelState === "unavailable") {
      return "unavailable";
    }

    return new Promise<CoValueCore>((resolve) => {
      const listener = (state: CoValueState) => {
        if (state.core) {
          resolve(state.core);
          this.removeListener(listener);
        }
      };

      this.addListener(listener);
    });
  }

  async loadFromPeers(peers: PeerState[]) {
    if (peers.length === 0) {
      return;
    }

    const loadAttempt = async (peersToLoadFrom: PeerState[]) => {
      const peersToActuallyLoadFrom = [];
      for (const peer of peersToLoadFrom) {
        const currentState = this.peers.get(peer.id);

        if (currentState?.type === "available") {
          continue;
        }

        if (currentState?.type === "errored") {
          continue;
        }

        if (
          currentState?.type === "unavailable" ||
          currentState?.type === "pending"
        ) {
          if (peer.shouldRetryUnavailableCoValues()) {
            this.markPending(peer.id);
            peersToActuallyLoadFrom.push(peer);
          }

          continue;
        }

        if (!currentState || currentState?.type === "unknown") {
          this.markPending(peer.id);
          peersToActuallyLoadFrom.push(peer);
        }
      }

      for (const peer of peersToActuallyLoadFrom) {
        if (peer.closed) {
          this.markNotFoundInPeer(peer.id);
          continue;
        }

        peer
          .pushOutgoingMessage({
            action: "load",
            ...(this.core ? this.core.knownState() : emptyKnownState(this.id)),
          })
          .catch((err) => {
            logger.warn(`Failed to push load message to peer ${peer.id}`, {
              err,
            });
          });

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

        const waitingForPeer = new Promise<void>((resolve) => {
          const markNotFound = () => {
            if (this.peers.get(peer.id)?.type === "pending") {
              this.markNotFoundInPeer(peer.id);
            }
          };

          const timeout = setTimeout(markNotFound, timeoutDuration);
          const removeCloseListener = peer.addCloseListener(markNotFound);

          const listener = (state: CoValueState) => {
            const peerState = state.peers.get(peer.id);
            if (
              state.isAvailable() || // might have become available from another peer e.g. through handleNewContent
              peerState?.type === "available" ||
              peerState?.type === "errored" ||
              peerState?.type === "unavailable"
            ) {
              state.removeListener(listener);
              removeCloseListener();
              clearTimeout(timeout);
              resolve();
            }
          };

          this.addListener(listener);
        });

        await waitingForPeer;
      }
    };

    await loadAttempt(peers);

    if (this.isAvailable()) {
      return;
    }

    // Retry loading from peers that have the retry flag enabled
    const peersWithRetry = peers.filter((p) =>
      p.shouldRetryUnavailableCoValues(),
    );

    if (peersWithRetry.length > 0) {
      const waitingForCoValue = new Promise<void>((resolve) => {
        const listener = (state: CoValueState) => {
          if (state.isAvailable()) {
            resolve();
            this.removeListener(listener);
          }
        };

        this.addListener(listener);
      });

      // We want to exit early if the coValue becomes available in between the retries
      await Promise.race([
        waitingForCoValue,
        runWithRetry(
          () => loadAttempt(peersWithRetry),
          CO_VALUE_LOADING_CONFIG.MAX_RETRIES,
        ),
      ]);
    }
  }

  private updateCounter(previousState: string | null) {
    const newState = this.highLevelState;

    if (previousState !== newState) {
      if (previousState) {
        this.counter.add(-1, { state: previousState });
      }
      this.counter.add(1, { state: newState });
    }
  }

  markNotFoundInPeer(peerId: PeerID) {
    const previousState = this.highLevelState;
    this.peers.set(peerId, { type: "unavailable" });
    this.updateCounter(previousState);
    this.notifyListeners();
  }

  // TODO: rename to "provided"
  markAvailable(coValue: CoValueCore, fromPeerId: PeerID) {
    const previousState = this.highLevelState;
    this.core = coValue;
    this.peers.set(fromPeerId, { type: "available" });
    this.updateCounter(previousState);
    this.notifyListeners();
  }

  internalMarkMagicallyAvailable(coValue: CoValueCore) {
    const previousState = this.highLevelState;
    this.core = coValue;
    this.updateCounter(previousState);
    this.notifyListeners();
  }

  markErrored(peerId: PeerID, error: TryAddTransactionsError) {
    const previousState = this.highLevelState;
    this.peers.set(peerId, { type: "errored", error });
    this.updateCounter(previousState);
    this.notifyListeners();
  }

  private markPending(peerId: PeerID) {
    const previousState = this.highLevelState;
    this.peers.set(peerId, { type: "pending" });
    this.updateCounter(previousState);
    this.notifyListeners();
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

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
