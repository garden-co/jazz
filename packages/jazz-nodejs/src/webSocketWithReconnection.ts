import { Peer } from "cojson";
import { createWebSocketPeer } from "cojson-transport-ws";

export function webSocketWithReconnection(
  peer: string,
  addPeer: (peer: Peer) => void,
  ws?: typeof WebSocket,
) {
  let done = false;

  const WebSocketConstructor = ws ?? WebSocket;

  const wsPeer = createWebSocketPeer({
    websocket: new WebSocketConstructor(peer),
    id: "upstream",
    role: "server",
    onClose: handleClose,
  });

  let timer: ReturnType<typeof setTimeout>;
  function handleClose() {
    if (done) return;

    clearTimeout(timer);
    timer = setTimeout(() => {
      const wsPeer: Peer = createWebSocketPeer({
        id: "upstream",
        websocket: new WebSocketConstructor(peer) as any,
        role: "server",
        onClose: handleClose,
      });

      addPeer(wsPeer);
    }, 1000);
  }

  return {
    peer: wsPeer,
    done: () => {
      done = true;
      clearTimeout(timer);
    },
  };
}
