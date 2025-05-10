import { Channel } from "queueueue";
import { Peer, PeerID, SyncMessage } from "./sync.js";
export { Channel } from "queueueue";

export function connectedPeers(
  peer1id: PeerID,
  peer2id: PeerID,
  {
    peer1role = "client",
    peer2role = "client",
    crashOnClose = false,
    deletePeerStateOnClose = true,
  }: {
    peer1role?: Peer["role"];
    peer2role?: Peer["role"];
    crashOnClose?: boolean;
    deletePeerStateOnClose?: boolean;
  } = {},
): [Peer, Peer] {
  const [from1to2Rx, from1to2Tx] = newQueuePair();
  const [from2to1Rx, from2to1Tx] = newQueuePair();

  const peer2AsPeer: Peer = {
    id: peer2id,
    incoming: from2to1Rx,
    outgoing: from1to2Tx,
    role: peer2role,
    crashOnClose: crashOnClose,
    deletePeerStateOnClose,
  };

  const peer1AsPeer: Peer = {
    id: peer1id,
    incoming: from1to2Rx,
    outgoing: from2to1Tx,
    role: peer1role,
    crashOnClose: crashOnClose,
    deletePeerStateOnClose,
  };

  return [peer1AsPeer, peer2AsPeer];
}

export function newQueuePair(): [
  AsyncIterable<SyncMessage>,
  Channel<SyncMessage>,
] {
  const channel = new Channel<SyncMessage>();

  return [channel.wrap(), channel];
}
