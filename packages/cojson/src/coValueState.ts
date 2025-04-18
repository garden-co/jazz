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
    | { type: "errored"; error: TryAddTransactionsError }
  >();

  private peersToRequestFrom = new Map<PeerID, PeerState>();
  loading = false;

  core: CoValueCore | null = null;
  id: RawCoID;

  listeners: Set<(state: CoValueState) => void> = new Set();

  constructor(id: RawCoID) {
    this.id = id;
  }

  addListener(listener: (state: CoValueState) => void) {
    this.listeners.add(listener);
    listener(this);
  }

  removeListener(listener: (state: CoValueState) => void) {
    this.listeners.delete(listener);
  }

  private notifyListeners() {
    this.loadFromNextPeer();

    for (const listener of this.listeners) {
      listener(this);
    }
  }

  async getCoValue() {
    if (this.core) {
      return this.core;
    }

    if (this.isDefinitelyUnavailable()) {
      return "unavailable";
    }

    return new Promise<CoValueCore | "unavailable">((resolve) => {
      const listener = (state: CoValueState) => {
        if (state.core) {
          resolve(state.core);
          this.removeListener(listener);
        } else if (this.isDefinitelyUnavailable()) {
          resolve("unavailable");
          this.removeListener(listener);
        }
      };

      this.addListener(listener);
    });
  }

  async loadFromPeers(peers: PeerState[]) {
    for (const peer of peers) {
      this.peersToRequestFrom.set(peer.id, peer);
    }

    this.loadFromNextPeer();
  }

  private loadFromNextPeer() {
    if (this.isLoading() || this.peersToRequestFrom.size === 0) {
      return;
    }

    // TODO: Load the peers with the same priority in parallel
    let selectedPeer: PeerState | undefined;

    for (const peer of this.peersToRequestFrom.values()) {
      const currentState = this.peers.get(peer.id);

      switch (currentState?.type) {
        case "available":
        case "errored":
        case "pending":
          this.peersToRequestFrom.delete(peer.id);
          continue;

        case "unavailable":
        case "unknown":
        default:
          if (
            !peer.shouldRetryUnavailableCoValues() &&
            currentState?.type === "unavailable"
          ) {
            this.peersToRequestFrom.delete(peer.id);
          } else if (
            !selectedPeer ||
            (peer.priority ?? 0) > (selectedPeer.priority ?? 0)
          ) {
            selectedPeer = peer;
          }
          break;
      }
    }

    if (!selectedPeer) {
      return;
    }

    this.peersToRequestFrom.delete(selectedPeer.id);
    this.peers.set(selectedPeer.id, { type: "pending" });

    const knownState = this.core
      ? this.core.knownState()
      : emptyKnownState(this.id);

    selectedPeer
      .pushOutgoingMessage({
        action: "load",
        ...knownState,
      })
      .catch((err) => {
        logger.warn(`Failed to push load message to peer ${selectedPeer.id}`, {
          err,
        });
      });
  }

  markNotFoundInPeer(peerId: PeerID) {
    this.peers.set(peerId, { type: "unavailable" });
    this.notifyListeners();
  }

  markAvailable(coValue: CoValueCore) {
    this.core = coValue;
    this.notifyListeners();
  }

  markErrored(peerId: PeerID, error: TryAddTransactionsError) {
    this.peers.set(peerId, { type: "errored", error });
    this.notifyListeners();
  }

  isErroredInPeer(peerId: PeerID) {
    return this.peers.get(peerId)?.type === "errored";
  }

  isAvailable(): this is { type: "available"; core: CoValueCore } {
    return !!this.core;
  }

  isUnknown() {
    if (this.core) {
      return false;
    }

    return this.peers.values().every((p) => p.type === "unknown");
  }

  isLoading() {
    return this.peers.values().some((p) => p.type === "pending");
  }

  isDefinitelyUnavailable() {
    if (this.core) {
      return false;
    }

    return (
      this.peers
        .values()
        .every((p) => p.type === "unavailable" || p.type === "errored") &&
      !this.isAvailable()
    );
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
