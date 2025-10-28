import {
  DisconnectedError,
  IncomingPeerChannel,
  OutgoingPeerChannel,
  Peer,
  PeerID,
  SyncMessage,
} from "./sync.js";

export function connectedPeers(
  peer1id: PeerID,
  peer2id: PeerID,
  {
    peer1role = "client",
    peer2role = "client",
    persistent = false,
  }: {
    peer1role?: Peer["role"];
    peer2role?: Peer["role"];
    persistent?: boolean;
  } = {},
): [Peer, Peer] {
  const from1to2 = new ConnectedPeerChannel();
  const from2to1 = new ConnectedPeerChannel();

  const peer2AsPeer: Peer = {
    id: peer2id,
    incoming: from2to1,
    outgoing: from1to2,
    role: peer2role,
    persistent,
  };

  const peer1AsPeer: Peer = {
    id: peer1id,
    incoming: from1to2,
    outgoing: from2to1,
    role: peer1role,
    persistent,
  };

  return [peer1AsPeer, peer2AsPeer];
}

export function newQueuePair(): [ConnectedPeerChannel, ConnectedPeerChannel] {
  const channel = new ConnectedPeerChannel();

  return [channel, channel];
}

export class ConnectedPeerChannel
  implements IncomingPeerChannel, OutgoingPeerChannel
{
  buffer: (SyncMessage | DisconnectedError)[] = [];

  push(msg: SyncMessage | DisconnectedError) {
    if (!this.listeners.size) {
      this.buffer.push(msg);
      return;
    }

    for (const listener of this.listeners) {
      listener(msg);
    }
  }

  close() {
    this.closed = true;
    for (const listener of this.closeListeners) {
      listener();
    }
    this.closeListeners.clear();
    this.listeners.clear();
  }

  listeners = new Set<(msg: SyncMessage | DisconnectedError) => void>();
  onMessage(callback: (msg: SyncMessage | DisconnectedError) => void) {
    if (this.buffer.length) {
      for (const msg of this.buffer) {
        callback(msg);
      }
      this.buffer = [];
    }

    this.listeners.add(callback);
  }

  closed = false;
  closeListeners = new Set<() => void>();
  onClose(callback: () => void) {
    this.closeListeners.add(callback);
  }
}
