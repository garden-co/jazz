import { PureJSCrypto } from "cojson/dist/crypto/PureJSCrypto";
import { WebSocketPeerWithReconnection } from "cojson-transport-ws";
import { createAnonymousJazzContext } from "jazz-tools";

export function createSSRJazzAgent(opts: { peer: string }) {
  const ssrNode = createAnonymousJazzContext({
    crypto: new PureJSCrypto(),
    peersToLoadFrom: [],
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
