import { CoValueCore } from "../coValueCore.js";
import { CO_VALUE_LOADING_TIMEOUT, CoValueEntry } from "../coValueEntry.js";
import { PeerEntry, Peers, getPeersWithoutErrors } from "../peer/index.js";
import { emptyKnownState } from "./types.js";

export class LoadService {
  constructor(private peers: Peers) {}
  /**
   * Sends "pull" request to peers to load/update the coValue state and request to subscribe to peer's updates if have not
   *
   * @param entry
   * @param peerToLoadFrom - Required peer to send the request to
   */
  async loadCoValue(
    entry: CoValueEntry,
    peerToLoadFrom?: PeerEntry,
  ): Promise<CoValueCore | "unavailable"> {
    const peers = peerToLoadFrom
      ? [peerToLoadFrom]
      : this.peers.getServerAndStorage();

    if (entry.state.type === "loading") {
      return entry.getCoValue();
    }

    try {
      await entry.loadFromPeers(
        getPeersWithoutErrors(peers, entry.id),
        loadCoValueFromPeers,
      );
    } catch (e) {
      console.error("Error loading from peers", entry.id, e);
    }

    return entry.getCoValue();
  }
}

async function loadCoValueFromPeers(
  coValueEntry: CoValueEntry,
  peers: PeerEntry[],
) {
  for await (const peer of peers) {
    if (coValueEntry.state.type === "available") {
      await peer.send.pull({
        knownState: coValueEntry.state.coValue.knownState(),
      });
    } else {
      await peer.send.pull({ knownState: emptyKnownState(coValueEntry.id) });
    }

    if (coValueEntry.state.type === "loading") {
      const timeout = setTimeout(() => {
        if (coValueEntry.state.type === "loading") {
          console.error(
            "Failed to load coValue from peer",
            peer.id,
            coValueEntry.id,
          );
          coValueEntry.markAsNotFoundInPeer(peer.id);
        }
      }, CO_VALUE_LOADING_TIMEOUT);

      coValueEntry.state.waitForPeer(peer.id).then(() => {
        clearTimeout(timeout);
      });
    }
  }
}
