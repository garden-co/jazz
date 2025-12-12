import { WebSocketPeerWithReconnection } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { createAnonymousJazzContext } from "jazz-tools";

export function createSSRJazzAgent(opts: { peer: string }) {
  const ssrNode = createAnonymousJazzContext({
    crypto: WasmCrypto.createSync(),
    peers: [],
  });

  const wsPeer = new WebSocketPeerWithReconnection({
    peer: opts.peer,
    reconnectionTimeout: 100,
    addPeer: (peer) => {
      ssrNode.agent.node.syncManager.addPeer(peer);
    },
    removePeer: () => {},
  });

  wsPeer.enable();

  return ssrNode.agent;
}
