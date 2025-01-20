import { CoValuesStore } from "../CoValuesStore.js";
import { CoValueEntry } from "../coValueEntry.js";
import { PeerEntry, PeerID } from "../peer/index.js";
import { CoValueKnownState, emptyKnownState } from "./types.js";

export class SyncService {
  constructor(
    private readonly onPushContent?: ({
      entry,
      peerId,
    }: { entry: CoValueEntry; peerId: PeerID }) => void,
  ) {}

  /**
   * Sends "push" request to peers to broadcast all known coValues state
   * and request to subscribe to those coValues updates (if have not)
   */
  async initialSync(
    peer: PeerEntry,
    coValuesStore: CoValuesStore,
  ): Promise<void> {
    const ids = coValuesStore.getOrderedIds();

    for (const id of ids) {
      const coValue = coValuesStore.expectCoValueLoaded(id);
      // Previously we used to send load + content,  see transformOutgoingMessageToPeer()
      await peer.send.push({
        peerKnownState: emptyKnownState(id),
        coValue,
      });

      // TODO M.O. should be moved inside peer.send.push
      if (this.onPushContent) {
        const entry = coValuesStore.get(coValue.id);
        this.onPushContent({ entry, peerId: peer.id });
      }
    }
  }

  /**
   * Sends "push" request to peers to broadcast the new known coValue state and request to subscribe to updates if have not
   */
  async syncCoValue(
    entry: CoValueEntry,
    peerKnownState: CoValueKnownState,
    peers: PeerEntry[],
  ) {
    if (entry.state.type !== "available") {
      throw new Error(`Can't sync unavailable coValue ${peerKnownState.id}`);
    }

    for await (const peer of peers) {
      if (peer.erroredCoValues.has(entry.id)) continue;

      await peer.send.push({
        peerKnownState,
        coValue: entry.state.coValue,
      });

      // TODO M.O. should be moved inside peer.send.push
      if (this.onPushContent) {
        this.onPushContent({ entry, peerId: peer.id });
      }
    }
  }
}
